"""
Unit tests for roda-search/handler.py.

Uses MagicMock to patch the module-level DynamoDB resource. No real AWS
calls are made.
"""

import importlib.util
import os
import sys
from unittest.mock import MagicMock, patch

REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")


def _load():
    """Load roda-search/handler.py as a unique module to avoid sys.modules collisions."""
    path = os.path.join(REPO_ROOT, "lambdas", "roda-search", "handler.py")
    spec = importlib.util.spec_from_file_location("_roda_search_handler", path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["_roda_search_handler"] = mod
    spec.loader.exec_module(mod)
    return mod


roda_search = _load()


# ---------------------------------------------------------------------------
# Test data
# ---------------------------------------------------------------------------

CLIMATE_ITEM = {
    "slug": "noaa-climate",
    "name": "NOAA Global Climate Data",
    "primaryTag": "climate",
    "tags": ["climate", "weather"],
    "description": "Historical climate measurements from NOAA stations worldwide.",
    "searchText": "climate weather temperature precipitation atmospheric",
    "formats": ["csv", "parquet"],
    "regions": ["us-east-1"],
    "s3Resources": [{"arn": "arn:aws:s3:::noaa-climate-data", "region": "us-east-1"}],
    "s3ResourceCount": 1,
    "registryUrl": "https://registry.opendata.aws/noaa-climate/",
    "license": "Open Data",
    "managedBy": "NOAA",
    "updateFrequency": "Daily",
    "documentation": "",
}

GENOME_ITEM = {
    "slug": "ncbi-genome",
    "name": "NCBI 1000 Genomes",
    "primaryTag": "genomics",
    "tags": ["genomics", "bioinformatics"],
    "description": "Whole genome sequencing data.",
    "searchText": "genomics dna rna sequencing variant genome",
    "formats": ["vcf", "bam"],
    "regions": ["us-east-1"],
    "s3Resources": [{"arn": "arn:aws:s3:::ncbi-genome-data", "region": "us-east-1"}],
    "s3ResourceCount": 1,
    "registryUrl": "",
    "license": "Open Data",
    "managedBy": "NCBI",
    "updateFrequency": "Periodic",
    "documentation": "",
}

ALL_ITEMS = [CLIMATE_ITEM, GENOME_ITEM]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_table(query_items=None, scan_items=None):
    table = MagicMock()
    table.query.return_value = {"Items": query_items or [], "Count": len(query_items or [])}
    table.scan.return_value = {"Items": scan_items or [], "Count": len(scan_items or [])}
    return table


def _patch_table(table):
    mock_ddb = MagicMock()
    mock_ddb.Table.return_value = table
    return patch.object(roda_search, "dynamodb", mock_ddb)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestEmptyQuery:
    def test_no_query_returns_all_via_scan(self):
        table = _make_table(scan_items=ALL_ITEMS)
        with _patch_table(table):
            result = roda_search.handler({}, None)
        assert result["count"] == 2
        assert len(result["datasets"]) == 2
        table.scan.assert_called_once()

    def test_response_has_required_keys(self):
        table = _make_table(scan_items=[CLIMATE_ITEM])
        with _patch_table(table):
            result = roda_search.handler({}, None)
        assert "count" in result
        assert "datasets" in result
        assert "query" in result
        assert "appliedTags" in result
        assert "appliedFormat" in result


class TestTagBasedSearch:
    def test_single_tag_uses_gsi_query(self):
        table = _make_table(query_items=[CLIMATE_ITEM])
        with _patch_table(table):
            result = roda_search.handler({"tags": ["climate"]}, None)
        table.query.assert_called_once()
        assert result["count"] == 1
        assert result["datasets"][0]["slug"] == "noaa-climate"

    def test_multiple_tags_uses_scan(self):
        table = _make_table(scan_items=ALL_ITEMS)
        with _patch_table(table):
            roda_search.handler({"tags": ["climate", "genomics"]}, None)
        table.scan.assert_called_once()

    def test_natural_language_infers_climate_tag(self):
        table = _make_table(query_items=[CLIMATE_ITEM])
        with _patch_table(table):
            result = roda_search.handler({"query": "find weather and temperature datasets"}, None)
        assert "climate" in result["appliedTags"]

    def test_natural_language_infers_genomics_tag(self):
        table = _make_table(query_items=[GENOME_ITEM])
        with _patch_table(table):
            result = roda_search.handler({"query": "genome sequencing data"}, None)
        assert "genomics" in result["appliedTags"]


class TestKeywordRanking:
    def test_keyword_match_returns_relevant_items(self):
        table = _make_table(query_items=ALL_ITEMS)
        with _patch_table(table):
            result = roda_search.handler({"query": "climate temperature", "tags": ["climate"]}, None)
        slugs = [d["slug"] for d in result["datasets"]]
        assert "noaa-climate" in slugs

    def test_no_keyword_match_preserves_tag_filtered_results(self):
        # When query keywords find no matches, keyword_rank falls back to returning
        # all tag-filtered results rather than silently discarding the filter output.
        table = _make_table(query_items=ALL_ITEMS)
        with _patch_table(table):
            result = roda_search.handler({"query": "xyz-nonexistent-keyword", "tags": ["climate"]}, None)
        assert result["count"] > 0


class TestFilters:
    def test_format_filter(self):
        table = _make_table(scan_items=ALL_ITEMS)
        with _patch_table(table):
            result = roda_search.handler({"format": "csv"}, None)
        assert all("csv" in d["formats"] for d in result["datasets"])
        assert result["count"] == 1

    def test_quicksight_compatible_filter(self):
        table = _make_table(scan_items=ALL_ITEMS)
        with _patch_table(table):
            result = roda_search.handler({"quicksight_compatible": True}, None)
        assert all(d["quicksightCompatible"] for d in result["datasets"])
        assert result["count"] == 1

    def test_max_results_enforced(self):
        many_items = [dict(CLIMATE_ITEM, slug=f"ds-{i}") for i in range(20)]
        table = _make_table(scan_items=many_items)
        with _patch_table(table):
            result = roda_search.handler({"max_results": 3}, None)
        assert result["count"] <= 3

    def test_max_results_capped_at_50(self):
        many_items = [dict(CLIMATE_ITEM, slug=f"ds-{i}") for i in range(60)]
        table = _make_table(scan_items=many_items)
        with _patch_table(table):
            result = roda_search.handler({"max_results": 100}, None)
        assert result["count"] <= 50


class TestProjectResult:
    def test_projected_fields_present(self):
        table = _make_table(scan_items=[CLIMATE_ITEM])
        with _patch_table(table):
            result = roda_search.handler({}, None)
        ds = result["datasets"][0]
        expected = {
            "slug", "name", "description", "tags", "formats",
            "license", "managedBy", "updateFrequency",
            "primaryBucket", "primaryRegion", "s3ResourceCount",
            "registryUrl", "quicksightCompatible", "documentation",
        }
        assert expected.issubset(ds.keys())

    def test_bucket_extracted_from_arn(self):
        table = _make_table(scan_items=[CLIMATE_ITEM])
        with _patch_table(table):
            result = roda_search.handler({}, None)
        assert result["datasets"][0]["primaryBucket"] == "noaa-climate-data"

    def test_description_truncated_at_400(self):
        long_item = dict(CLIMATE_ITEM, description="x" * 500)
        table = _make_table(scan_items=[long_item])
        with _patch_table(table):
            result = roda_search.handler({}, None)
        assert len(result["datasets"][0]["description"]) <= 400


class TestDynamoDBErrors:
    def test_scan_exception_returns_empty(self):
        table = MagicMock()
        table.scan.side_effect = Exception("DynamoDB timeout")
        with _patch_table(table):
            result = roda_search.handler({}, None)
        assert result["count"] == 0
        assert result["datasets"] == []

    def test_gsi_exception_falls_back_to_empty(self):
        table = MagicMock()
        table.query.side_effect = Exception("GSI not found")
        with _patch_table(table):
            result = roda_search.handler({"tags": ["climate"]}, None)
        assert result["count"] == 0
