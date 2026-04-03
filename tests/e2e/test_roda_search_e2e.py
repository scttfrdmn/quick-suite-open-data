"""
E2E tests for roda-search Lambda against deployed QuickSuiteOpenData stack.

Uses DynamoDB-seeded synthetic items (e2e-test-* slugs) so results are
deterministic regardless of the real RODA catalog state.
"""

import pytest

from tests.e2e.conftest import invoke

pytestmark = pytest.mark.e2e


class TestRodaSearchE2E:
    def test_tag_query_returns_seeded_item(self, lam, tool_arns, seeded_catalog):
        """GSI tag query for 'climate' returns at least the seeded item."""
        result = invoke(lam, tool_arns["roda_search"], {"tags": ["climate"]})
        slugs = [d["slug"] for d in result.get("datasets", [])]
        assert "e2e-test-climate" in slugs, f"Expected e2e-test-climate in {slugs}"

    def test_keyword_query_matches_searchtext(self, lam, tool_arns, seeded_catalog):
        """Keyword query matching seeded searchText returns the item."""
        result = invoke(lam, tool_arns["roda_search"], {"query": "e2e synth atmospheric"})
        slugs = [d["slug"] for d in result.get("datasets", [])]
        assert "e2e-test-climate" in slugs

    def test_response_has_required_keys(self, lam, tool_arns, seeded_catalog):
        """Response always includes count, datasets, query, appliedTags, appliedFormat, next_token."""
        result = invoke(lam, tool_arns["roda_search"], {"tags": ["climate"]})
        for key in ("count", "datasets", "query", "appliedTags", "appliedFormat", "next_token"):
            assert key in result, f"Missing key '{key}' in response: {result}"

    def test_quality_score_present_on_every_result(self, lam, tool_arns, seeded_catalog):
        """Every returned dataset includes a quality_score dict with freshness."""
        result = invoke(lam, tool_arns["roda_search"], {"tags": ["climate"]})
        for dataset in result.get("datasets", []):
            qs = dataset.get("quality_score")
            assert qs is not None, f"Missing quality_score on {dataset.get('slug')}"
            assert "freshness" in qs

    def test_max_results_non_integer_returns_error(self, lam, tool_arns):
        """Non-integer max_results returns an error dict (not a Lambda error)."""
        result = invoke(lam, tool_arns["roda_search"], {"query": "climate", "max_results": "abc"})
        assert "error" in result

    def test_max_results_out_of_range_returns_error(self, lam, tool_arns):
        """max_results=0 is out of range — returns error."""
        result = invoke(lam, tool_arns["roda_search"], {"query": "climate", "max_results": 0})
        assert "error" in result

    def test_max_results_over_50_returns_error(self, lam, tool_arns):
        """max_results=51 is over the cap — returns error."""
        result = invoke(lam, tool_arns["roda_search"], {"query": "climate", "max_results": 51})
        assert "error" in result

    def test_empty_query_returns_empty_not_error(self, lam, tool_arns, seeded_catalog):
        """Empty query with an unused tag returns empty datasets without an error key."""
        result = invoke(lam, tool_arns["roda_search"], {"tags": ["nonexistent-tag-xyz"]})
        assert "error" not in result
        assert result["count"] == 0

    def test_quicksight_compatible_filter_excludes_non_qs_formats(self, lam, tool_arns, seeded_catalog):
        """quicksight_compatible=True excludes items with only vcf/bam formats."""
        result = invoke(lam, tool_arns["roda_search"],
                        {"tags": ["genomics"], "quicksight_compatible": True})
        # e2e-test-genomics has formats [vcf, bam] — should be excluded
        slugs = [d["slug"] for d in result.get("datasets", [])]
        assert "e2e-test-genomics" not in slugs

    def test_format_filter_applied(self, lam, tool_arns, seeded_catalog):
        """Format filter for 'parquet' returns only items with parquet format."""
        result = invoke(lam, tool_arns["roda_search"], {"tags": ["climate"], "format": "parquet"})
        for dataset in result.get("datasets", []):
            assert "parquet" in dataset.get("formats", []), \
                f"Item {dataset.get('slug')} missing parquet in formats"

    def test_invalid_pagination_token_returns_error(self, lam, tool_arns):
        """Garbage pagination token returns an error dict."""
        result = invoke(lam, tool_arns["roda_search"],
                        {"query": "climate", "pagination_token": "not-valid-base64!!!"})
        assert "error" in result

    def test_pagination_token_round_trip(self, lam, tool_arns, seeded_catalog):
        """A next_token from one response can be used in the next request."""
        # Request max_results=1 to force pagination if multiple climate items exist
        first = invoke(lam, tool_arns["roda_search"], {"tags": ["climate"], "max_results": 1})
        token = first.get("next_token", "")
        if not token:
            pytest.skip("Only one result page — cannot test pagination round-trip")
        second = invoke(lam, tool_arns["roda_search"],
                        {"tags": ["climate"], "max_results": 1, "pagination_token": token})
        assert "error" not in second
        assert "datasets" in second

    def test_pagination_real_catalog(self, lam, tool_arns, roda_sync_result):
        """
        With a large real RODA catalog (500+ items), multi-page scan returns
        non-overlapping result sets and valid next_token values.

        Uses the session-scoped roda_sync_result fixture (catalog-sync runs once per session).
        Skipped if sync produced fewer than 50 items (e.g. RODA format changed again).
        """
        import json as _json
        body = _json.loads(roda_sync_result.get("body", "{}"))
        if body.get("synced", 0) < 50:
            pytest.skip(f"Not enough synced RODA items for pagination test: {body}")

        # Page 1: broad scan with no filter, max_results=10
        page1 = invoke(lam, tool_arns["roda_search"], {"max_results": 10})
        assert "error" not in page1, f"Page 1 error: {page1}"
        assert page1["count"] == 10, f"Expected 10 results on page 1: {page1}"
        token = page1.get("next_token", "")
        assert token, "Expected next_token with 500+ catalog items and max_results=10"

        # Page 2: use token from page 1
        page2 = invoke(lam, tool_arns["roda_search"],
                       {"max_results": 10, "pagination_token": token})
        assert "error" not in page2, f"Page 2 error: {page2}"
        assert page2["count"] > 0, "Page 2 returned no results"

        # No duplicate slugs across the two pages
        slugs1 = {d["slug"] for d in page1["datasets"]}
        slugs2 = {d["slug"] for d in page2["datasets"]}
        overlap = slugs1 & slugs2
        assert not overlap, f"Duplicate slugs across pages 1 and 2: {overlap}"

        # Every result on both pages has a quality_score
        for dataset in page1["datasets"] + page2["datasets"]:
            assert "quality_score" in dataset, \
                f"Missing quality_score on {dataset.get('slug')}"
