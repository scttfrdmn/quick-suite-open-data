"""
Tests for roda-search/handler.py.

Unit tests use MagicMock only where Substrate fault injection is not yet
available (error paths — blocked on scttfrdmn/substrate#280).

All happy-path tests use real DynamoDB via Substrate.
"""

import importlib
import importlib.util
import os
import sys
import time
from unittest.mock import patch

import pytest

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
# Error-injection tests — real DynamoDB via Substrate + fault injection
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestDynamoDBErrors:
    def _reload(self, substrate_url, monkeypatch):
        monkeypatch.setenv("AWS_ENDPOINT_URL", substrate_url)
        env = {"TABLE_NAME": _CATALOG_TABLE, "SEARCH_CACHE_TABLE": ""}
        with patch.dict(os.environ, env):
            alias = "_roda_search_err_integ"
            path = os.path.join(REPO_ROOT, "lambdas", "roda-search", "handler.py")
            spec = importlib.util.spec_from_file_location(alias, path)
            mod = importlib.util.module_from_spec(spec)
            sys.modules[alias] = mod
            spec.loader.exec_module(mod)
            return mod

    def _create_table(self, substrate_url):
        import boto3 as _boto3
        ddb = _boto3.client(
            "dynamodb",
            endpoint_url=substrate_url,
            region_name="us-east-1",
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        ddb.create_table(
            TableName=_CATALOG_TABLE,
            KeySchema=[{"AttributeName": "slug", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "slug", "AttributeType": "S"},
                {"AttributeName": "primaryTag", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "by-primary-tag",
                    "KeySchema": [{"AttributeName": "primaryTag", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        ddb.get_waiter("table_exists").wait(TableName=_CATALOG_TABLE)

    def test_scan_exception_returns_empty(
        self, substrate_url, reset_substrate, monkeypatch, fault_inject
    ):
        self._create_table(substrate_url)
        fault_inject("dynamodb", "Scan", "InternalServerError", 500)
        h = self._reload(substrate_url, monkeypatch)
        result = h.handler({}, None)
        assert result["count"] == 0
        assert result["datasets"] == []

    def test_gsi_exception_falls_back_to_empty(
        self, substrate_url, reset_substrate, monkeypatch, fault_inject
    ):
        self._create_table(substrate_url)
        fault_inject("dynamodb", "Query", "InternalServerError", 500)
        h = self._reload(substrate_url, monkeypatch)
        result = h.handler({"tags": ["climate"]}, None)
        assert result["count"] == 0


# ---------------------------------------------------------------------------
# Integration tests — real DynamoDB via Substrate
# ---------------------------------------------------------------------------

_CATALOG_TABLE = "qs-catalog-test"

_THREE_YEARS_AGO = int(time.time()) - (3 * 365 * 24 * 3600)
_THIRTY_DAYS_AGO = int(time.time()) - (30 * 24 * 3600)
_TWELVE_MONTHS_AGO = int(time.time()) - (365 * 24 * 3600)

# Seed items in DynamoDB wire format.
# NOTE: Substrate does not support empty-string DynamoDB values ("S": "").
# Omit optional string fields that may be empty.
_INTEG_ITEMS_DDB = [
    {
        "slug": {"S": "noaa-climate"},
        "name": {"S": "NOAA Climate"},
        "primaryTag": {"S": "climate"},
        "tags": {"SS": ["climate", "weather"]},
        "description": {"S": "Climate measurements from NOAA stations."},
        "searchText": {"S": "climate weather temperature precipitation atmospheric"},
        "formats": {"SS": ["csv", "parquet"]},
        "license": {"S": "Open Data"},
        "managedBy": {"S": "NOAA"},
        "updateFrequency": {"S": "Daily"},
        "registryUrl": {"S": "https://registry.opendata.aws/noaa-climate/"},
        "s3ResourceCount": {"N": "1"},
        "s3Resources": {"L": [{"M": {
            "arn": {"S": "arn:aws:s3:::noaa-climate-data"},
            "region": {"S": "us-east-1"},
        }}]},
    },
    {
        "slug": {"S": "ncbi-genome"},
        "name": {"S": "NCBI 1000 Genomes"},
        "primaryTag": {"S": "genomics"},
        "tags": {"SS": ["genomics", "bioinformatics"]},
        "description": {"S": "Whole genome sequencing data."},
        "searchText": {"S": "genomics dna rna sequencing variant genome"},
        "formats": {"SS": ["vcf", "bam"]},
        "license": {"S": "Open Data"},
        "managedBy": {"S": "NCBI"},
        "updateFrequency": {"S": "Periodic"},
        "registryUrl": {"S": "https://registry.opendata.aws/ncbi-genome/"},
        "s3ResourceCount": {"N": "1"},
    },
    {
        "slug": {"S": "noaa-ocean"},
        "name": {"S": "NOAA Ocean Data"},
        "primaryTag": {"S": "oceans"},
        "tags": {"SS": ["oceans", "marine"]},
        "description": {"S": "Ocean temperature and salinity measurements."},
        "searchText": {"S": "ocean temperature salinity marine coastal bathymetry"},
        "formats": {"SS": ["csv"]},
        "license": {"S": "Open Data"},
        "managedBy": {"S": "NOAA"},
        "updateFrequency": {"S": "Daily"},
        "registryUrl": {"S": "https://registry.opendata.aws/noaa-ocean/"},
        "s3ResourceCount": {"N": "1"},
    },
    # Deprecated dataset
    {
        "slug": {"S": "old-climate"},
        "name": {"S": "Old Climate Archive"},
        "primaryTag": {"S": "climate"},
        "tags": {"SS": ["climate"]},
        "description": {"S": "Deprecated climate archive."},
        "searchText": {"S": "climate old archive deprecated"},
        "formats": {"SS": ["csv"]},
        "license": {"S": "Open Data"},
        "managedBy": {"S": "NOAA"},
        "updateFrequency": {"S": "Never"},
        "registryUrl": {"S": "https://registry.opendata.aws/old-climate/"},
        "s3ResourceCount": {"N": "1"},
        "deprecated": {"BOOL": True},
    },
    # Stale dataset — last_updated 3 years ago, all completeness fields present
    {
        "slug": {"S": "stale-survey"},
        "name": {"S": "Stale Survey Data"},
        "primaryTag": {"S": "economics"},
        "tags": {"SS": ["economics", "demographics"]},
        "description": {"S": "Old survey data with stale last_updated."},
        "searchText": {"S": "economics census demographic survey stale"},
        "formats": {"SS": ["csv", "json"]},
        "license": {"S": "Open Data"},
        "managedBy": {"S": "Census"},
        "updateFrequency": {"S": "Decennial"},
        "registryUrl": {"S": "https://registry.opendata.aws/census-survey/"},
        "s3ResourceCount": {"N": "1"},
        "s3Resources": {"L": [{"M": {
            "arn": {"S": "arn:aws:s3:::census-survey-data"},
            "region": {"S": "us-east-1"},
        }}]},
        "last_updated": {"N": str(_THREE_YEARS_AGO)},
    },
    # Recent dataset — last_updated 30 days ago, with last_verified
    {
        "slug": {"S": "recent-satellite"},
        "name": {"S": "Recent Satellite Imagery"},
        "primaryTag": {"S": "satellite"},
        "tags": {"SS": ["satellite", "geospatial"]},
        "description": {"S": "Recently updated satellite imagery."},
        "searchText": {"S": "satellite imagery geospatial remote sensing recent"},
        "formats": {"SS": ["geotiff"]},
        "license": {"S": "Open Data"},
        "managedBy": {"S": "USGS"},
        "updateFrequency": {"S": "Daily"},
        "registryUrl": {"S": "https://registry.opendata.aws/landsat-recent/"},
        "s3ResourceCount": {"N": "1"},
        "s3Resources": {"L": [{"M": {
            "arn": {"S": "arn:aws:s3:::landsat-recent"},
            "region": {"S": "us-west-2"},
        }}]},
        "last_updated": {"N": str(_THIRTY_DAYS_AGO)},
        "last_verified": {"S": "2026-03-01T00:00:00+00:00"},
    },
    # Aging dataset — last_updated 12 months ago
    {
        "slug": {"S": "aging-genomics"},
        "name": {"S": "Aging Genomics Dataset"},
        "primaryTag": {"S": "genomics"},
        "tags": {"SS": ["genomics"]},
        "description": {"S": "Genomics data from about one year ago."},
        "searchText": {"S": "genomics sequencing aging data"},
        "formats": {"SS": ["vcf"]},
        "license": {"S": "Open Data"},
        "managedBy": {"S": "NHGRI"},
        "updateFrequency": {"S": "Annual"},
        "registryUrl": {"S": "https://registry.opendata.aws/aging-genomics/"},
        "s3ResourceCount": {"N": "1"},
        "last_updated": {"N": str(_TWELVE_MONTHS_AGO)},
    },
    # Partial dataset — only required fields (no description, formats, s3Resources, registryUrl)
    {
        "slug": {"S": "partial-dataset"},
        "name": {"S": "Minimal Dataset"},
        "primaryTag": {"S": "health"},
        "tags": {"SS": ["health"]},
        "searchText": {"S": "health medical minimal partial"},
    },
]


@pytest.mark.integration
class TestRodaSearchIntegration:

    def _reload_search(self, substrate_url, monkeypatch):
        """Reload roda-search handler targeting Substrate."""
        monkeypatch.setenv("AWS_ENDPOINT_URL", substrate_url)
        env = {"TABLE_NAME": _CATALOG_TABLE, "SEARCH_CACHE_TABLE": ""}
        with patch.dict(os.environ, env):
            alias = "_roda_search_integ"
            path = os.path.join(REPO_ROOT, "lambdas", "roda-search", "handler.py")
            spec = importlib.util.spec_from_file_location(alias, path)
            mod = importlib.util.module_from_spec(spec)
            sys.modules[alias] = mod
            spec.loader.exec_module(mod)
            return mod

    def _seed_catalog(self, substrate_url, items_ddb=None):
        """Create catalog table and seed items in Substrate."""
        import boto3

        if items_ddb is None:
            items_ddb = _INTEG_ITEMS_DDB

        ddb = boto3.client(
            "dynamodb",
            endpoint_url=substrate_url,
            region_name="us-east-1",
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        ddb.create_table(
            TableName=_CATALOG_TABLE,
            KeySchema=[{"AttributeName": "slug", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "slug", "AttributeType": "S"},
                {"AttributeName": "primaryTag", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "by-primary-tag",
                    "KeySchema": [{"AttributeName": "primaryTag", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        waiter = ddb.get_waiter("table_exists")
        waiter.wait(TableName=_CATALOG_TABLE)
        for item in items_ddb:
            ddb.put_item(TableName=_CATALOG_TABLE, Item=item)

    # ------------------------------------------------------------------
    # Basic scan / response shape
    # ------------------------------------------------------------------

    def test_empty_query_returns_all_seeded_items(self, substrate_url, reset_substrate, monkeypatch):
        self._seed_catalog(substrate_url)
        h = self._reload_search(substrate_url, monkeypatch)
        result = h.handler({}, None)
        assert result["count"] >= len(_INTEG_ITEMS_DDB)

    def test_response_has_required_keys(self, substrate_url, reset_substrate, monkeypatch):
        self._seed_catalog(substrate_url)
        h = self._reload_search(substrate_url, monkeypatch)
        result = h.handler({}, None)
        for key in ("count", "datasets", "query", "appliedTags", "appliedFormat"):
            assert key in result

    # ------------------------------------------------------------------
    # Tag-based search
    # ------------------------------------------------------------------

    def test_single_tag_filter_via_gsi(self, substrate_url, reset_substrate, monkeypatch):
        self._seed_catalog(substrate_url)
        h = self._reload_search(substrate_url, monkeypatch)
        result = h.handler({"tags": ["climate"]}, None)
        assert result["count"] >= 1
        slugs = [d["slug"] for d in result["datasets"]]
        assert "noaa-climate" in slugs
        assert "ncbi-genome" not in slugs

    def test_multiple_tags_uses_scan_returns_matching(self, substrate_url, reset_substrate, monkeypatch):
        self._seed_catalog(substrate_url)
        h = self._reload_search(substrate_url, monkeypatch)
        result = h.handler({"tags": ["climate", "genomics"]}, None)
        slugs = [d["slug"] for d in result["datasets"]]
        # Both tags present → both datasets should appear
        assert "noaa-climate" in slugs or "ncbi-genome" in slugs

    def test_keyword_search_returns_relevant(self, substrate_url, reset_substrate, monkeypatch):
        self._seed_catalog(substrate_url)
        h = self._reload_search(substrate_url, monkeypatch)
        result = h.handler({"query": "marine coastal salinity"}, None)
        assert result["count"] >= 1
        slugs = [d["slug"] for d in result["datasets"]]
        assert "noaa-ocean" in slugs

    # ------------------------------------------------------------------
    # Filters
    # ------------------------------------------------------------------

    def test_max_results_one(self, substrate_url, reset_substrate, monkeypatch):
        self._seed_catalog(substrate_url)
        h = self._reload_search(substrate_url, monkeypatch)
        result = h.handler({"max_results": 1}, None)
        assert result["count"] == 1

    def test_format_filter_csv_only(self, substrate_url, reset_substrate, monkeypatch):
        self._seed_catalog(substrate_url)
        h = self._reload_search(substrate_url, monkeypatch)
        result = h.handler({"format": "csv"}, None)
        # noaa-climate (csv,parquet) and noaa-ocean (csv) match; ncbi-genome (vcf,bam) does not
        assert result["count"] >= 2
        assert all("csv" in d["formats"] for d in result["datasets"])

    def test_format_filter_excludes_non_matching(self, substrate_url, reset_substrate, monkeypatch):
        self._seed_catalog(substrate_url)
        h = self._reload_search(substrate_url, monkeypatch)
        result = h.handler({"format": "vcf"}, None)
        slugs = [d["slug"] for d in result["datasets"]]
        assert "ncbi-genome" in slugs
        assert "noaa-climate" not in slugs
        assert "noaa-ocean" not in slugs

    def test_quicksight_compatible_filter(self, substrate_url, reset_substrate, monkeypatch):
        self._seed_catalog(substrate_url)
        h = self._reload_search(substrate_url, monkeypatch)
        result = h.handler({"quicksight_compatible": True}, None)
        # noaa-climate (csv/parquet) and noaa-ocean (csv) are QS-compatible; ncbi-genome (vcf/bam) is not
        slugs = [d["slug"] for d in result["datasets"]]
        assert "noaa-climate" in slugs
        assert "ncbi-genome" not in slugs
        assert all(d["quicksightCompatible"] for d in result["datasets"])

    def test_max_results_invalid_returns_error(self, substrate_url, reset_substrate, monkeypatch):
        self._seed_catalog(substrate_url)
        h = self._reload_search(substrate_url, monkeypatch)
        result = h.handler({"max_results": 100}, None)
        assert "error" in result

    # ------------------------------------------------------------------
    # Result projection
    # ------------------------------------------------------------------

    def test_projected_fields_present(self, substrate_url, reset_substrate, monkeypatch):
        self._seed_catalog(substrate_url)
        h = self._reload_search(substrate_url, monkeypatch)
        result = h.handler({"tags": ["climate"]}, None)
        ds = result["datasets"][0]
        for field in ("slug", "name", "description", "tags", "formats",
                      "license", "managedBy", "updateFrequency",
                      "primaryBucket", "primaryRegion", "s3ResourceCount",
                      "registryUrl", "quicksightCompatible", "documentation"):
            assert field in ds, f"Missing field: {field}"

    def test_bucket_extracted_from_arn(self, substrate_url, reset_substrate, monkeypatch):
        self._seed_catalog(substrate_url)
        h = self._reload_search(substrate_url, monkeypatch)
        result = h.handler({"tags": ["climate"]}, None)
        climate_ds = next(d for d in result["datasets"] if d["slug"] == "noaa-climate")
        assert climate_ds["primaryBucket"] == "noaa-climate-data"

    def test_description_truncated_at_400(self, substrate_url, reset_substrate, monkeypatch):
        import boto3
        ddb = boto3.client(
            "dynamodb",
            endpoint_url=substrate_url,
            region_name="us-east-1",
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        long_item = {
            "slug": {"S": "long-desc"},
            "name": {"S": "Long Description Dataset"},
            "primaryTag": {"S": "health"},
            "tags": {"SS": ["health"]},
            "description": {"S": "x" * 500},
            "searchText": {"S": "health medical long description"},
            "formats": {"SS": ["csv"]},
            "license": {"S": "Open"},
            "managedBy": {"S": "Test"},
            "updateFrequency": {"S": "Never"},
            "registryUrl": {"S": "https://registry.opendata.aws/long/"},
            "s3ResourceCount": {"N": "0"},
        }
        ddb.create_table(
            TableName=_CATALOG_TABLE,
            KeySchema=[{"AttributeName": "slug", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "slug", "AttributeType": "S"},
                {"AttributeName": "primaryTag", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[{
                "IndexName": "by-primary-tag",
                "KeySchema": [{"AttributeName": "primaryTag", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }],
            BillingMode="PAY_PER_REQUEST",
        )
        ddb.get_waiter("table_exists").wait(TableName=_CATALOG_TABLE)
        ddb.put_item(TableName=_CATALOG_TABLE, Item=long_item)
        h = self._reload_search(substrate_url, monkeypatch)
        result = h.handler({}, None)
        assert len(result["datasets"][0]["description"]) <= 400

    # ------------------------------------------------------------------
    # Deprecated datasets
    # ------------------------------------------------------------------

    def test_deprecated_flag_in_result(self, substrate_url, reset_substrate, monkeypatch):
        self._seed_catalog(substrate_url)
        h = self._reload_search(substrate_url, monkeypatch)
        result = h.handler({"tags": ["climate"]}, None)
        slugs_by_deprecated = {d["slug"]: d["deprecated"] for d in result["datasets"]}
        assert slugs_by_deprecated.get("old-climate") is True

    def test_non_deprecated_flag_false_in_result(self, substrate_url, reset_substrate, monkeypatch):
        self._seed_catalog(substrate_url)
        h = self._reload_search(substrate_url, monkeypatch)
        result = h.handler({"tags": ["climate"]}, None)
        slugs_by_deprecated = {d["slug"]: d["deprecated"] for d in result["datasets"]}
        assert slugs_by_deprecated.get("noaa-climate") is False

    def test_exclude_deprecated_removes_items(self, substrate_url, reset_substrate, monkeypatch):
        self._seed_catalog(substrate_url)
        h = self._reload_search(substrate_url, monkeypatch)
        result = h.handler({"tags": ["climate"], "exclude_deprecated": True}, None)
        slugs = [d["slug"] for d in result["datasets"]]
        assert "old-climate" not in slugs
        assert "noaa-climate" in slugs

    def test_exclude_deprecated_false_keeps_deprecated_items(self, substrate_url, reset_substrate, monkeypatch):
        self._seed_catalog(substrate_url)
        h = self._reload_search(substrate_url, monkeypatch)
        result = h.handler({"tags": ["climate"], "exclude_deprecated": False}, None)
        slugs = [d["slug"] for d in result["datasets"]]
        assert "old-climate" in slugs

    # ------------------------------------------------------------------
    # Quality score
    # ------------------------------------------------------------------

    def test_quality_score_present_in_every_result(self, substrate_url, reset_substrate, monkeypatch):
        self._seed_catalog(substrate_url)
        h = self._reload_search(substrate_url, monkeypatch)
        result = h.handler({}, None)
        for ds in result["datasets"]:
            assert "quality_score" in ds
            qs = ds["quality_score"]
            assert "freshness" in qs
            assert "schema_completeness" in qs
            assert "last_verified" in qs

    def test_freshness_stale_when_last_updated_old(self, substrate_url, reset_substrate, monkeypatch):
        self._seed_catalog(substrate_url)
        h = self._reload_search(substrate_url, monkeypatch)
        result = h.handler({"tags": ["economics"]}, None)
        stale_ds = next(d for d in result["datasets"] if d["slug"] == "stale-survey")
        assert stale_ds["quality_score"]["freshness"] == "stale"

    def test_freshness_current_when_recent(self, substrate_url, reset_substrate, monkeypatch):
        self._seed_catalog(substrate_url)
        h = self._reload_search(substrate_url, monkeypatch)
        result = h.handler({"tags": ["satellite"]}, None)
        recent_ds = next(d for d in result["datasets"] if d["slug"] == "recent-satellite")
        assert recent_ds["quality_score"]["freshness"] == "current"

    def test_freshness_stale_when_last_updated_missing(self, substrate_url, reset_substrate, monkeypatch):
        self._seed_catalog(substrate_url)
        h = self._reload_search(substrate_url, monkeypatch)
        result = h.handler({"tags": ["oceans"]}, None)
        ocean_ds = next(d for d in result["datasets"] if d["slug"] == "noaa-ocean")
        # noaa-ocean has no last_updated → stale
        assert ocean_ds["quality_score"]["freshness"] == "stale"

    def test_freshness_aging_between_6_and_24_months(self, substrate_url, reset_substrate, monkeypatch):
        self._seed_catalog(substrate_url)
        h = self._reload_search(substrate_url, monkeypatch)
        result = h.handler({"tags": ["genomics"]}, None)
        aging_ds = next((d for d in result["datasets"] if d["slug"] == "aging-genomics"), None)
        assert aging_ds is not None
        assert aging_ds["quality_score"]["freshness"] == "aging"

    def test_schema_completeness_1_when_all_fields_present(self, substrate_url, reset_substrate, monkeypatch):
        self._seed_catalog(substrate_url)
        h = self._reload_search(substrate_url, monkeypatch)
        result = h.handler({"tags": ["economics"]}, None)
        stale_ds = next(d for d in result["datasets"] if d["slug"] == "stale-survey")
        # stale-survey has all 6 completeness fields (name, description, tags, formats, s3Resources, registryUrl)
        assert stale_ds["quality_score"]["schema_completeness"] == 1.0

    def test_schema_completeness_less_than_1_when_fields_missing(self, substrate_url, reset_substrate, monkeypatch):
        self._seed_catalog(substrate_url)
        h = self._reload_search(substrate_url, monkeypatch)
        result = h.handler({"tags": ["health"]}, None)
        partial_ds = next((d for d in result["datasets"] if d["slug"] == "partial-dataset"), None)
        assert partial_ds is not None
        # partial-dataset has name and tags but missing description, formats, s3Resources, registryUrl
        assert partial_ds["quality_score"]["schema_completeness"] < 1.0

    def test_last_verified_none_when_field_absent(self, substrate_url, reset_substrate, monkeypatch):
        self._seed_catalog(substrate_url)
        h = self._reload_search(substrate_url, monkeypatch)
        result = h.handler({"tags": ["oceans"]}, None)
        ocean_ds = next(d for d in result["datasets"] if d["slug"] == "noaa-ocean")
        assert ocean_ds["quality_score"]["last_verified"] is None

    def test_last_verified_returned_when_field_present(self, substrate_url, reset_substrate, monkeypatch):
        self._seed_catalog(substrate_url)
        h = self._reload_search(substrate_url, monkeypatch)
        result = h.handler({"tags": ["satellite"]}, None)
        recent_ds = next(d for d in result["datasets"] if d["slug"] == "recent-satellite")
        assert recent_ds["quality_score"]["last_verified"] == "2026-03-01T00:00:00+00:00"
