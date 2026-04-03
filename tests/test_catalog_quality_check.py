"""
Tests for catalog-quality-check/handler.py.

Happy-path tests use real DynamoDB via Substrate.
S3 error-injection tests use Substrate fault injection (substrate#280 resolved).
_FakeCW is kept permanently — Substrate has no CloudWatch metrics readback API.
"""

import importlib.util
import os
import sys
import time
from unittest.mock import MagicMock, patch

import boto3
import pytest

REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")

TWO_YEARS_SECONDS = 2 * 365 * 24 * 3600
_QC_TABLE = "qs-qc-test"


# ---------------------------------------------------------------------------
# Stubs
# ---------------------------------------------------------------------------

class _FakeCW:
    """CloudWatch stub that records put_metric_data calls."""

    def __init__(self):
        self.calls = []

    def put_metric_data(self, **kwargs):
        self.calls.append(kwargs)


# ---------------------------------------------------------------------------
# Handler factory
# ---------------------------------------------------------------------------

def _make_handler(substrate_url, monkeypatch, items, table_name=_QC_TABLE):
    """
    Load the quality-check handler against real Substrate DynamoDB.
    Returns (mod, table_resource, fake_cw).

    mod.s3_anon is set to a MagicMock that returns {} for head_bucket by
    default.  Override mod.s3_anon in the test for S3-specific scenarios.
    """
    ddb_client = boto3.client(
        "dynamodb",
        endpoint_url=substrate_url,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    ddb_client.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "slug", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "slug", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb_client.get_waiter("table_exists").wait(TableName=table_name)

    ddb_resource = boto3.resource(
        "dynamodb",
        endpoint_url=substrate_url,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    table = ddb_resource.Table(table_name)
    for item in items:
        table.put_item(Item=item)

    fake_cw = _FakeCW()
    monkeypatch.setenv("AWS_ENDPOINT_URL", substrate_url)
    env = {"TABLE_NAME": table_name}
    with patch.dict(os.environ, env):
        alias = "_catalog_qc_integ"
        path = os.path.join(REPO_ROOT, "lambdas", "catalog-quality-check", "handler.py")
        spec = importlib.util.spec_from_file_location(alias, path)
        mod = importlib.util.module_from_spec(spec)
        sys.modules[alias] = mod
        spec.loader.exec_module(mod)

    mod.cw = fake_cw
    default_s3 = MagicMock()
    default_s3.head_bucket.return_value = {}
    mod.s3_anon = default_s3

    return mod, table, fake_cw


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestCatalogQualityCheck:
    def test_missing_last_updated_marked_stale(self, substrate_url, reset_substrate, monkeypatch):
        """Item with no last_updated field → marked stale, count = 1."""
        items = [{"slug": "no-timestamp"}]
        mod, table, cw = _make_handler(substrate_url, monkeypatch, items)

        result = mod.handler({}, None)

        assert result["stale_count"] == 1
        assert result["scanned"] == 1
        item = table.get_item(Key={"slug": "no-timestamp"})["Item"]
        assert item.get("stale") is True

    def test_old_last_updated_marked_stale(self, substrate_url, reset_substrate, monkeypatch):
        """Item with last_updated > 2 years ago → marked stale."""
        old_ts = int(time.time()) - TWO_YEARS_SECONDS - 86400
        items = [{"slug": "old-dataset", "last_updated": old_ts}]
        mod, table, cw = _make_handler(substrate_url, monkeypatch, items)

        result = mod.handler({}, None)

        assert result["stale_count"] == 1
        item = table.get_item(Key={"slug": "old-dataset"})["Item"]
        assert item.get("stale") is True

    def test_recent_last_updated_not_stale(self, substrate_url, reset_substrate, monkeypatch):
        """Item with recent last_updated → NOT marked stale (but last_verified written)."""
        recent_ts = int(time.time()) - 86400
        items = [{"slug": "fresh-dataset", "last_updated": recent_ts}]
        mod, table, cw = _make_handler(substrate_url, monkeypatch, items)

        result = mod.handler({}, None)

        assert result["stale_count"] == 0
        assert result["scanned"] == 1
        item = table.get_item(Key={"slug": "fresh-dataset"})["Item"]
        assert "last_verified" in item
        assert "stale" not in item

    def test_boundary_exactly_two_years_is_stale(self, substrate_url, reset_substrate, monkeypatch):
        """Item just past the 2-year cutoff → stale."""
        now = int(time.time())
        boundary_ts = now - TWO_YEARS_SECONDS - 1
        items = [{"slug": "boundary-dataset", "last_updated": boundary_ts}]
        mod, table, cw = _make_handler(substrate_url, monkeypatch, items)

        result = mod.handler({}, None)

        assert result["stale_count"] == 1
        item = table.get_item(Key={"slug": "boundary-dataset"})["Item"]
        assert item.get("stale") is True

    def test_boundary_just_under_two_years_not_stale(self, substrate_url, reset_substrate, monkeypatch):
        """Item just under 2 years old → NOT stale."""
        now = int(time.time())
        boundary_ts = now - TWO_YEARS_SECONDS + 1
        items = [{"slug": "under-boundary", "last_updated": boundary_ts}]
        mod, table, cw = _make_handler(substrate_url, monkeypatch, items)

        result = mod.handler({}, None)

        assert result["stale_count"] == 0
        item = table.get_item(Key={"slug": "under-boundary"})["Item"]
        assert "stale" not in item

    def test_cloudwatch_metric_emitted(self, substrate_url, reset_substrate, monkeypatch):
        """StaleDatasets metric is published to CloudWatch with the stale count."""
        items = [{"slug": "stale-one"}, {"slug": "stale-two"}]
        mod, table, cw = _make_handler(substrate_url, monkeypatch, items)

        mod.handler({}, None)

        assert len(cw.calls) == 1
        call = cw.calls[0]
        assert call["Namespace"] == "QuickSuiteOpenData"
        metric = call["MetricData"][0]
        assert metric["MetricName"] == "StaleDatasets"
        assert metric["Value"] == 2

    def test_mixed_items_correct_counts(self, substrate_url, reset_substrate, monkeypatch):
        """Mix of stale and fresh items — counts match."""
        now = int(time.time())
        items = [
            {"slug": "fresh-a", "last_updated": now - 86400},
            {"slug": "stale-b"},
            {"slug": "stale-c", "last_updated": now - TWO_YEARS_SECONDS - 86400},
            {"slug": "fresh-d", "last_updated": now - 30 * 86400},
        ]
        mod, table, cw = _make_handler(substrate_url, monkeypatch, items)

        result = mod.handler({}, None)

        assert result["scanned"] == 4
        assert result["stale_count"] == 2

        stale_b = table.get_item(Key={"slug": "stale-b"})["Item"]
        stale_c = table.get_item(Key={"slug": "stale-c"})["Item"]
        fresh_a = table.get_item(Key={"slug": "fresh-a"})["Item"]
        fresh_d = table.get_item(Key={"slug": "fresh-d"})["Item"]

        assert stale_b.get("stale") is True
        assert stale_c.get("stale") is True
        assert "stale" not in fresh_a
        assert "stale" not in fresh_d


@pytest.mark.integration
class TestS3Reachability:
    """Tests for S3 bucket reachability probing (unreachable flag)."""

    def _make_s3_item(self, slug, bucket):
        return {
            "slug": slug,
            "last_updated": int(time.time()) - 86400,
            "s3Resources": [{"arn": f"arn:aws:s3:::{bucket}"}],
        }

    def test_missing_s3_bucket_marked_unreachable(
        self, substrate_url, reset_substrate, monkeypatch, fault_inject
    ):
        """head_bucket returns 404 → item flagged unreachable, count = 1."""
        items = [self._make_s3_item("missing-bucket-dataset", "nonexistent-roda-bucket")]
        mod, table, cw = _make_handler(substrate_url, monkeypatch, items)

        # Inject a 404 for S3 HeadBucket; inject a real Substrate S3 client
        fault_inject("s3", "HeadBucket", "NoSuchBucket", 404)
        s3_substrate = boto3.client(
            "s3",
            endpoint_url=substrate_url,
            region_name="us-east-1",
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        mod.s3_anon = s3_substrate

        result = mod.handler({}, None)

        assert result["unreachable_count"] == 1
        assert result["stale_count"] == 0
        item = table.get_item(Key={"slug": "missing-bucket-dataset"})["Item"]
        assert item.get("unreachable") is True

    def test_reachable_s3_bucket_not_marked(self, substrate_url, reset_substrate, monkeypatch):
        """head_bucket succeeds → item NOT flagged unreachable."""
        items = [self._make_s3_item("live-bucket-dataset", "active-roda-bucket")]
        mod, table, cw = _make_handler(substrate_url, monkeypatch, items)

        # Create the bucket in Substrate and inject a real S3 client as s3_anon
        s3_substrate = boto3.client(
            "s3",
            endpoint_url=substrate_url,
            region_name="us-east-1",
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        s3_substrate.create_bucket(Bucket="active-roda-bucket")
        mod.s3_anon = s3_substrate

        result = mod.handler({}, None)

        assert result["unreachable_count"] == 0
        item = table.get_item(Key={"slug": "live-bucket-dataset"})["Item"]
        assert "unreachable" not in item

    def test_missing_s3_resources_skips_probe(self, substrate_url, reset_substrate, monkeypatch):
        """Item with no s3Resources → probe never called, unreachable_count = 0."""
        items = [{"slug": "no-s3-resources", "last_updated": int(time.time()) - 86400}]
        mod, table, cw = _make_handler(substrate_url, monkeypatch, items)

        result = mod.handler({}, None)

        assert result["unreachable_count"] == 0
        mod.s3_anon.head_bucket.assert_not_called()
        item = table.get_item(Key={"slug": "no-s3-resources"})["Item"]
        assert "last_verified" in item
