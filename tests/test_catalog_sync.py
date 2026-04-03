"""
Tests for catalog-sync/handler.py.

Pure-logic tests (transform_dataset, derive_slug, detect_formats, handler routing)
use no AWS calls.  Error-injection and happy-path tests use real S3 + DynamoDB via
Substrate (fault injection via /v1/fault/rules for error paths).
"""

import importlib
import importlib.util
import json
import os
import sys
from unittest.mock import MagicMock, patch

import boto3
import pytest

REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")


def _load():
    """Load catalog-sync/handler.py as a unique module."""
    path = os.path.join(REPO_ROOT, "lambdas", "catalog-sync", "handler.py")
    spec = importlib.util.spec_from_file_location("_catalog_sync_handler", path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["_catalog_sync_handler"] = mod
    spec.loader.exec_module(mod)
    return mod


handler_mod = _load()


# ---------------------------------------------------------------------------
# Sample dataset fixtures
# ---------------------------------------------------------------------------

SAMPLE_DATASET = {
    "Name": "NOAA Global Climate Data",
    "Description": "Historical temperature and precipitation records in CSV and Parquet format.",
    "Tags": ["climate", "weather", "noaa", "aws-pds"],
    "License": "Open Data",
    "ManagedBy": "NOAA",
    "UpdateFrequency": "Daily",
    "Contact": "https://registry.opendata.aws/noaa-climate/",
    "Documentation": "https://docs.noaa.gov/climate",
    "Resources": [
        {
            "Type": "S3 Bucket",
            "ARN": "arn:aws:s3:::noaa-climate-bucket",
            "Region": "us-east-1",
            "Description": "CSV and Parquet files with climate measurements",
            "RequesterPays": False,
            "AccountRequired": False,
            "Explore": ["https://noaa-climate-bucket.s3.amazonaws.com/"],
        }
    ],
    "DataAtWork": {
        "Tutorials": [
            {"Title": "Analyzing Climate Data", "URL": "https://example.com/tutorial1"},
            {"Title": "Parquet with Pandas", "URL": "https://example.com/tutorial2"},
        ],
        "Publications": [
            {"Title": "Climate Change Study", "URL": "https://example.com/pub1"},
        ],
    },
}

MINIMAL_DATASET = {
    "Name": "Minimal Dataset",
    "Tags": [],
    "Resources": [],
    "Description": "",
}

NAMELESS_DATASET = {
    "Name": "",
    "Tags": [],
    "Resources": [],
}


# ---------------------------------------------------------------------------
# transform_dataset() unit tests
# ---------------------------------------------------------------------------

class TestTransformDataset:
    def test_basic_fields_present(self):
        item = handler_mod.transform_dataset(SAMPLE_DATASET, "roda/ndjson/noaa-climate.ndjson")
        assert item["name"] == "NOAA Global Climate Data"
        assert item["license"] == "Open Data"
        assert item["managedBy"] == "NOAA"
        assert item["updateFrequency"] == "Daily"

    def test_slug_derived_from_source_key(self):
        item = handler_mod.transform_dataset(SAMPLE_DATASET, "roda/ndjson/noaa-climate.ndjson")
        assert item["slug"] == "noaa-climate"

    def test_primary_tag_skips_aws_pds(self):
        item = handler_mod.transform_dataset(SAMPLE_DATASET, "roda/ndjson/x.ndjson")
        assert item["primaryTag"] == "climate"

    def test_primary_tag_fallback_when_only_aws_pds(self):
        ds = {**SAMPLE_DATASET, "Tags": ["aws-pds"]}
        item = handler_mod.transform_dataset(ds, "roda/ndjson/x.ndjson")
        assert item["primaryTag"] == "uncategorized"

    def test_primary_tag_uncategorized_when_no_tags(self):
        item = handler_mod.transform_dataset(MINIMAL_DATASET, "roda/ndjson/minimal.ndjson")
        assert item["primaryTag"] == "uncategorized"

    def test_s3_resources_extracted(self):
        item = handler_mod.transform_dataset(SAMPLE_DATASET, "roda/ndjson/x.ndjson")
        assert len(item["s3Resources"]) == 1
        assert item["s3Resources"][0]["arn"] == "arn:aws:s3:::noaa-climate-bucket"
        assert item["s3ResourceCount"] == 1

    def test_formats_detected_from_description(self):
        item = handler_mod.transform_dataset(SAMPLE_DATASET, "roda/ndjson/x.ndjson")
        assert "csv" in item["formats"]
        assert "parquet" in item["formats"]

    def test_search_text_includes_name_description_tags(self):
        item = handler_mod.transform_dataset(SAMPLE_DATASET, "roda/ndjson/x.ndjson")
        assert "noaa global climate data" in item["searchText"]
        assert "climate" in item["searchText"]

    def test_description_truncated_at_2000(self):
        long_ds = {**SAMPLE_DATASET, "Description": "x" * 5000}
        item = handler_mod.transform_dataset(long_ds, "roda/ndjson/x.ndjson")
        assert len(item["description"]) == 2000

    def test_tutorials_included(self):
        item = handler_mod.transform_dataset(SAMPLE_DATASET, "roda/ndjson/x.ndjson")
        assert len(item["tutorials"]) == 2

    def test_publications_included(self):
        item = handler_mod.transform_dataset(SAMPLE_DATASET, "roda/ndjson/x.ndjson")
        assert len(item["publications"]) == 1

    def test_returns_none_for_nameless_dataset(self):
        result = handler_mod.transform_dataset(NAMELESS_DATASET, "roda/ndjson/x.ndjson")
        assert result is None

    def test_registry_url_constructed(self):
        item = handler_mod.transform_dataset(SAMPLE_DATASET, "roda/ndjson/noaa-climate.ndjson")
        assert item["registryUrl"] == "https://registry.opendata.aws/noaa-climate/"

    def test_source_key_stored(self):
        item = handler_mod.transform_dataset(SAMPLE_DATASET, "roda/ndjson/noaa-climate.ndjson")
        assert item["sourceKey"] == "roda/ndjson/noaa-climate.ndjson"

    def test_last_updated_written(self):
        import time
        before = int(time.time())
        item = handler_mod.transform_dataset(SAMPLE_DATASET, "roda/ndjson/noaa-climate.ndjson")
        after = int(time.time())
        assert "last_updated" in item
        assert before <= item["last_updated"] <= after

    def test_last_updated_is_integer(self):
        item = handler_mod.transform_dataset(SAMPLE_DATASET, "roda/ndjson/noaa-climate.ndjson")
        assert isinstance(item["last_updated"], int)


# ---------------------------------------------------------------------------
# derive_slug() unit tests
# ---------------------------------------------------------------------------

class TestDeriveSlug:
    def test_slug_from_ndjson_key(self):
        slug = handler_mod.derive_slug("Any Name", "roda/ndjson/1000-genomes.ndjson")
        assert slug == "1000-genomes"

    def test_slug_from_json_key(self):
        slug = handler_mod.derive_slug("Any Name", "roda/ndjson/landsat-8.json")
        assert slug == "landsat-8"

    def test_slug_from_name_fallback(self):
        slug = handler_mod.derive_slug("NOAA Climate Data", "roda/ndjson/x.ndjson")
        # 'x' is too short (<=2 chars) so falls back to name
        assert slug == "noaa-climate-data"

    def test_slug_lowercased(self):
        slug = handler_mod.derive_slug("My Dataset", "roda/ndjson/ab.ndjson")
        # 'ab' is too short, falls back to name
        assert slug == "my-dataset"

    def test_slug_special_chars_removed(self):
        slug = handler_mod.derive_slug("CO₂ & Climate (2024)", "roda/ndjson/x.ndjson")
        assert " " not in slug
        assert "&" not in slug

    def test_slug_max_128_chars(self):
        long_name = "a" * 200
        slug = handler_mod.derive_slug(long_name, "roda/ndjson/x.ndjson")
        assert len(slug) <= 128


# ---------------------------------------------------------------------------
# detect_formats() unit tests
# ---------------------------------------------------------------------------

class TestDetectFormats:
    def test_csv_detected_from_description(self):
        formats = handler_mod.detect_formats([], "This dataset is available in CSV format.")
        assert "csv" in formats

    def test_parquet_detected(self):
        formats = handler_mod.detect_formats([], "Data available as Parquet files.")
        assert "parquet" in formats

    def test_formats_from_resource_description(self):
        resources = [{"Description": "VCF files for genomic variants", "Explore": []}]
        formats = handler_mod.detect_formats(resources, "")
        assert "vcf" in formats

    def test_netcdf_pattern(self):
        formats = handler_mod.detect_formats([], "climate data in NetCDF (.nc) files")
        assert "netcdf" in formats

    def test_returns_sorted_list(self):
        formats = handler_mod.detect_formats([], "csv and parquet and json")
        assert formats == sorted(formats)

    def test_no_false_positives(self):
        formats = handler_mod.detect_formats([], "no data formats mentioned here")
        assert formats == []

    def test_deduplication(self):
        formats = handler_mod.detect_formats([], "csv CSV csv")
        assert formats.count("csv") == 1


# ---------------------------------------------------------------------------
# handle_full_sync() / handle_sns_update() error-injection tests
# (kept as MagicMock — blocked on scttfrdmn/substrate#280)
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestHandleFullSync:
    """S3 error during full sync — Substrate fault injection."""

    def test_full_sync_handles_s3_error_gracefully(
        self, substrate_url, reset_substrate, monkeypatch, fault_inject
    ):
        # Create DDB table and S3 bucket; put a valid ndjson key so the
        # paginator returns one entry, then inject GetObject failure.
        import boto3 as _boto3

        ddb = _boto3.client(
            "dynamodb",
            endpoint_url=substrate_url,
            region_name="us-east-1",
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        ddb.create_table(
            TableName=_SYNC_TABLE,
            KeySchema=[{"AttributeName": "slug", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "slug", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        ddb.get_waiter("table_exists").wait(TableName=_SYNC_TABLE)

        s3 = _boto3.client(
            "s3",
            endpoint_url=substrate_url,
            region_name="us-east-1",
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        s3.create_bucket(Bucket=_SYNC_BUCKET)
        s3.put_object(
            Bucket=_SYNC_BUCKET,
            Key=f"{_SYNC_PREFIX}broken.ndjson",
            Body=b"placeholder",
        )

        fault_inject("s3", "GetObject", "InternalError", 500)

        monkeypatch.setenv("AWS_ENDPOINT_URL", substrate_url)
        env = {
            "TABLE_NAME": _SYNC_TABLE,
            "RODA_BUCKET": _SYNC_BUCKET,
            "RODA_PREFIX": _SYNC_PREFIX,
        }
        with patch.dict(os.environ, env):
            alias = "_catalog_sync_err_integ"
            path = os.path.join(REPO_ROOT, "lambdas", "catalog-sync", "handler.py")
            spec = importlib.util.spec_from_file_location(alias, path)
            mod = importlib.util.module_from_spec(spec)
            sys.modules[alias] = mod
            spec.loader.exec_module(mod)

        result = mod.handle_full_sync(mod.dynamodb.Table(_SYNC_TABLE))
        body = json.loads(result["body"])
        assert body["errors"] == 1
        assert body["synced"] == 0


@pytest.mark.integration
class TestHandleSnsUpdate:
    """S3 error during SNS update — Substrate fault injection."""

    def _sns_event(self, s3_key):
        s3_notification = json.dumps({
            "Records": [{"s3": {"object": {"key": s3_key}}}]
        })
        return {
            "Records": [{
                "EventSource": "aws:sns",
                "Sns": {"Message": s3_notification},
            }]
        }

    def test_sns_update_handles_s3_error_gracefully(
        self, substrate_url, reset_substrate, monkeypatch, fault_inject
    ):
        import boto3 as _boto3

        ddb = _boto3.client(
            "dynamodb",
            endpoint_url=substrate_url,
            region_name="us-east-1",
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        ddb.create_table(
            TableName=_SYNC_TABLE,
            KeySchema=[{"AttributeName": "slug", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "slug", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        ddb.get_waiter("table_exists").wait(TableName=_SYNC_TABLE)

        s3 = _boto3.client(
            "s3",
            endpoint_url=substrate_url,
            region_name="us-east-1",
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        s3.create_bucket(Bucket=_SYNC_BUCKET)

        fault_inject("s3", "GetObject", "InternalError", 500)

        monkeypatch.setenv("AWS_ENDPOINT_URL", substrate_url)
        env = {
            "TABLE_NAME": _SYNC_TABLE,
            "RODA_BUCKET": _SYNC_BUCKET,
            "RODA_PREFIX": _SYNC_PREFIX,
        }
        with patch.dict(os.environ, env):
            alias = "_catalog_sync_sns_err_integ"
            path = os.path.join(REPO_ROOT, "lambdas", "catalog-sync", "handler.py")
            spec = importlib.util.spec_from_file_location(alias, path)
            mod = importlib.util.module_from_spec(spec)
            sys.modules[alias] = mod
            spec.loader.exec_module(mod)

        key = f"{_SYNC_PREFIX}noaa-climate.ndjson"
        result = mod.handle_sns_update(self._sns_event(key), mod.dynamodb.Table(_SYNC_TABLE))
        assert result["statusCode"] == 200
        # No items should have been written (GetObject failed)
        resp = ddb.scan(TableName=_SYNC_TABLE)
        assert len(resp.get("Items", [])) == 0


# ---------------------------------------------------------------------------
# handler() event routing tests
# ---------------------------------------------------------------------------

class TestHandlerRouting:
    def test_scheduledevent_triggers_full_sync(self):
        mock_table = MagicMock()
        scheduled_event = {"source": "aws.events", "detail-type": "Scheduled Event"}

        with patch.object(handler_mod, "dynamodb") as mock_ddb, \
             patch.object(handler_mod, "handle_full_sync") as mock_full_sync, \
             patch.object(handler_mod, "handle_sns_update") as mock_sns:
            mock_ddb.Table.return_value = mock_table
            mock_full_sync.return_value = {"statusCode": 200, "body": json.dumps({"synced": 0, "errors": 0})}
            handler_mod.handler(scheduled_event, None)

        mock_full_sync.assert_called_once_with(mock_table)
        mock_sns.assert_not_called()

    def test_sns_event_triggers_incremental_update(self):
        mock_table = MagicMock()
        sns_event = {
            "Records": [
                {"EventSource": "aws:sns", "Sns": {"Message": json.dumps({"Records": []})}}
            ]
        }

        with patch.object(handler_mod, "dynamodb") as mock_ddb, \
             patch.object(handler_mod, "handle_full_sync") as mock_full_sync, \
             patch.object(handler_mod, "handle_sns_update") as mock_sns:
            mock_ddb.Table.return_value = mock_table
            mock_sns.return_value = {"statusCode": 200}
            handler_mod.handler(sns_event, None)

        mock_sns.assert_called_once()
        mock_full_sync.assert_not_called()


# ---------------------------------------------------------------------------
# Integration tests — real S3 + DynamoDB via Substrate
# ---------------------------------------------------------------------------

_SYNC_TABLE = "qs-catalog-sync-test"
_SYNC_BUCKET = "roda-catalog-test"
_SYNC_PREFIX = "ndjson/"

_SYNC_SAMPLE_DATASET = {
    "Name": "NOAA Global Climate Data",
    "Description": "Historical temperature and precipitation records in CSV and Parquet format.",
    "Tags": ["climate", "weather", "noaa"],
    "License": "Open Data",
    "ManagedBy": "NOAA",
    "UpdateFrequency": "Daily",
    "Resources": [
        {
            "Type": "S3 Bucket",
            "ARN": "arn:aws:s3:::noaa-climate-bucket",
            "Region": "us-east-1",
            "Description": "CSV and Parquet files",
            "RequesterPays": False,
            "AccountRequired": False,
        }
    ],
}

_SYNC_SECOND_DATASET = {
    "Name": "Landsat 8 Imagery",
    "Description": "GeoTIFF satellite imagery from Landsat 8.",
    "Tags": ["satellite", "geospatial"],
    "License": "Open Data",
    "ManagedBy": "USGS",
    "UpdateFrequency": "Daily",
    "Resources": [],
}


@pytest.mark.integration
class TestCatalogSyncIntegration:

    def _setup(self, substrate_url):
        import boto3 as _boto3

        ddb = _boto3.client(
            "dynamodb",
            endpoint_url=substrate_url,
            region_name="us-east-1",
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        ddb.create_table(
            TableName=_SYNC_TABLE,
            KeySchema=[{"AttributeName": "slug", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "slug", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        ddb.get_waiter("table_exists").wait(TableName=_SYNC_TABLE)

        s3 = _boto3.client(
            "s3",
            endpoint_url=substrate_url,
            region_name="us-east-1",
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        s3.create_bucket(Bucket=_SYNC_BUCKET)
        return ddb, s3

    def _reload(self, substrate_url, monkeypatch):
        import importlib.util as _ilu

        monkeypatch.setenv("AWS_ENDPOINT_URL", substrate_url)
        env = {
            "TABLE_NAME": _SYNC_TABLE,
            "RODA_BUCKET": _SYNC_BUCKET,
            "RODA_PREFIX": _SYNC_PREFIX,
        }
        with patch.dict(os.environ, env):
            alias = "_catalog_sync_integ"
            path = os.path.join(REPO_ROOT, "lambdas", "catalog-sync", "handler.py")
            spec = _ilu.spec_from_file_location(alias, path)
            mod = _ilu.module_from_spec(spec)
            sys.modules[alias] = mod
            spec.loader.exec_module(mod)
        return mod

    def _put_ndjson(self, s3_client, key, datasets):
        ndjson = "\n".join(json.dumps(d) for d in datasets).encode()
        s3_client.put_object(Bucket=_SYNC_BUCKET, Key=key, Body=ndjson)

    def test_full_sync_writes_item_to_dynamodb(self, substrate_url, reset_substrate, monkeypatch):
        ddb, s3 = self._setup(substrate_url)
        self._put_ndjson(s3, f"{_SYNC_PREFIX}noaa-climate.ndjson", [_SYNC_SAMPLE_DATASET])
        mod = self._reload(substrate_url, monkeypatch)
        mod.handle_full_sync(mod.dynamodb.Table(_SYNC_TABLE))
        resp = ddb.get_item(
            TableName=_SYNC_TABLE,
            Key={"slug": {"S": "noaa-climate"}},
        )
        assert "Item" in resp
        assert resp["Item"]["name"]["S"] == "NOAA Global Climate Data"

    def test_full_sync_returns_synced_count(self, substrate_url, reset_substrate, monkeypatch):
        ddb, s3 = self._setup(substrate_url)
        self._put_ndjson(s3, f"{_SYNC_PREFIX}noaa-climate.ndjson", [_SYNC_SAMPLE_DATASET])
        mod = self._reload(substrate_url, monkeypatch)
        result = mod.handle_full_sync(mod.dynamodb.Table(_SYNC_TABLE))
        body = json.loads(result["body"])
        assert body["synced"] == 1
        assert body["errors"] == 0

    def test_full_sync_skips_non_ndjson_keys(self, substrate_url, reset_substrate, monkeypatch):
        ddb, s3 = self._setup(substrate_url)
        s3.put_object(Bucket=_SYNC_BUCKET, Key=f"{_SYNC_PREFIX}readme.txt", Body=b"not a dataset")
        self._put_ndjson(s3, f"{_SYNC_PREFIX}noaa-climate.ndjson", [_SYNC_SAMPLE_DATASET])
        mod = self._reload(substrate_url, monkeypatch)
        result = mod.handle_full_sync(mod.dynamodb.Table(_SYNC_TABLE))
        body = json.loads(result["body"])
        assert body["synced"] == 1

    def test_full_sync_multiple_datasets_in_one_file(self, substrate_url, reset_substrate, monkeypatch):
        ddb, s3 = self._setup(substrate_url)
        self._put_ndjson(s3, f"{_SYNC_PREFIX}multi.ndjson", [_SYNC_SAMPLE_DATASET, _SYNC_SECOND_DATASET])
        mod = self._reload(substrate_url, monkeypatch)
        result = mod.handle_full_sync(mod.dynamodb.Table(_SYNC_TABLE))
        body = json.loads(result["body"])
        assert body["synced"] == 2
        assert body["errors"] == 0

    def test_full_sync_skips_empty_lines(self, substrate_url, reset_substrate, monkeypatch):
        ddb, s3 = self._setup(substrate_url)
        content = (
            json.dumps(_SYNC_SAMPLE_DATASET) + "\n\n" + json.dumps(_SYNC_SECOND_DATASET) + "\n"
        ).encode()
        s3.put_object(Bucket=_SYNC_BUCKET, Key=f"{_SYNC_PREFIX}spaced.ndjson", Body=content)
        mod = self._reload(substrate_url, monkeypatch)
        result = mod.handle_full_sync(mod.dynamodb.Table(_SYNC_TABLE))
        body = json.loads(result["body"])
        assert body["synced"] == 2

    def test_sns_update_writes_item(self, substrate_url, reset_substrate, monkeypatch):
        ddb, s3 = self._setup(substrate_url)
        key = f"{_SYNC_PREFIX}noaa-climate.ndjson"
        self._put_ndjson(s3, key, [_SYNC_SAMPLE_DATASET])
        mod = self._reload(substrate_url, monkeypatch)
        s3_notification = json.dumps({"Records": [{"s3": {"object": {"key": key}}}]})
        event = {
            "Records": [{"EventSource": "aws:sns", "Sns": {"Message": s3_notification}}]
        }
        mod.handle_sns_update(event, mod.dynamodb.Table(_SYNC_TABLE))
        resp = ddb.get_item(
            TableName=_SYNC_TABLE,
            Key={"slug": {"S": "noaa-climate"}},
        )
        assert "Item" in resp

    def test_sns_update_returns_200(self, substrate_url, reset_substrate, monkeypatch):
        ddb, s3 = self._setup(substrate_url)
        key = f"{_SYNC_PREFIX}noaa-climate.ndjson"
        self._put_ndjson(s3, key, [_SYNC_SAMPLE_DATASET])
        mod = self._reload(substrate_url, monkeypatch)
        s3_notification = json.dumps({"Records": [{"s3": {"object": {"key": key}}}]})
        event = {
            "Records": [{"EventSource": "aws:sns", "Sns": {"Message": s3_notification}}]
        }
        result = mod.handle_sns_update(event, mod.dynamodb.Table(_SYNC_TABLE))
        assert result["statusCode"] == 200
