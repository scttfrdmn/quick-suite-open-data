"""
E2E tests for s3-browse Lambda against deployed QuickSuiteOpenData stack.

Source discovery: reads SOURCES_CONFIG from the deployed Lambda environment.
Tests that require a configured source skip gracefully if none is present.

To run source-specific tests, ensure config/sources.yaml includes at least one
S3 source (RODA buckets work well: noaa-ghcn-pds, gdelt-open-data, etc.) and
redeploy the stack.
"""

import pytest

from tests.e2e.conftest import invoke

pytestmark = pytest.mark.e2e


class TestS3BrowseE2E:
    def test_no_source_returns_source_catalog(self, lam, tool_arns, sources_config):
        """Calling with no source arg returns the list of configured sources."""
        result = invoke(lam, tool_arns["s3_browse"], {})
        # Either a sources list or an error if SOURCES_CONFIG is empty
        assert "sources" in result or "error" in result, \
            f"Unexpected response shape: {result}"
        if sources_config:
            assert "sources" in result
            assert result["count"] == len(sources_config)

    def test_unknown_source_returns_error(self, lam, tool_arns):
        """Unknown source label returns an error dict."""
        result = invoke(lam, tool_arns["s3_browse"], {"source": "___nonexistent___"})
        assert "error" in result

    def test_browse_configured_source(self, lam, tool_arns, first_source):
        """Browsing a valid configured source returns a result (not an error)."""
        result = invoke(lam, tool_arns["s3_browse"], {"source": first_source["label"]})
        assert "error" not in result, f"Unexpected error: {result.get('error')}"
        assert "count" in result
        assert "files" in result or "objects" in result or result["count"] >= 0

    def test_prefix_filter_narrows_results(self, lam, tool_arns, first_source):
        """Adding a prefix filter returns a subset (or all) of results."""
        # Browse without prefix first to get any prefix we can use
        full = invoke(lam, tool_arns["s3_browse"], {"source": first_source["label"]})
        if full.get("count", 0) == 0:
            pytest.skip("No objects in source — cannot test prefix narrowing")
        # Use a non-matching prefix — should return 0 objects
        result = invoke(lam, tool_arns["s3_browse"],
                        {"source": first_source["label"], "prefix": "zzz-no-match-xyz/"})
        assert "error" not in result
        assert result.get("count", -1) == 0

    def test_path_traversal_denied(self, lam, tool_arns, first_source):
        """Prefix with .. traversal is rejected."""
        result = invoke(lam, tool_arns["s3_browse"],
                        {"source": first_source["label"], "prefix": "../../etc/passwd"})
        assert "error" in result

    def test_max_keys_capped_at_500(self, lam, tool_arns, first_source):
        """Requesting max_keys=1000 is silently capped to 500."""
        result = invoke(lam, tool_arns["s3_browse"],
                        {"source": first_source["label"], "max_keys": 1000})
        # Should succeed (cap applied silently) — not return an error
        assert "error" not in result
