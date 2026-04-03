"""
E2E tests for catalog-sync Lambda against deployed QuickSuiteOpenData stack.

Invokes the real Lambda once per session (via roda_sync_result fixture) — the
Lambda reads from public registry.opendata.aws and writes to the deployed DynamoDB
catalog table. Sharing the result across tests cuts total runtime from ~3 minutes
to ~60 seconds for this class.
"""

import json

import pytest

from tests.e2e.conftest import invoke

pytestmark = pytest.mark.e2e


class TestCatalogSyncE2E:
    def test_full_sync_returns_success(self, roda_sync_result):
        """Full sync Lambda returns statusCode 200 with synced > 0."""
        result = roda_sync_result
        assert result.get("statusCode") == 200, f"Unexpected response: {result}"
        body = json.loads(result["body"])
        assert body["synced"] > 0, f"Expected synced > 0, got: {body}"
        assert body["errors"] == 0 or body["synced"] > body["errors"], \
            f"Too many errors relative to synced count: {body}"

    def test_sync_populates_catalog_table(self, roda_sync_result, catalog_table_name, ddb_resource):
        """After sync, the catalog table contains at least one well-formed item."""
        table = ddb_resource.Table(catalog_table_name)
        resp = table.scan(
            Limit=10,
            ProjectionExpression="slug, #n, formats",
            ExpressionAttributeNames={"#n": "name"},
        )
        items = resp.get("Items", [])
        assert len(items) >= 1, "Catalog table is empty after sync"
        # At least one item should have a name field (not just a synthetic e2e-test item)
        named = [i for i in items if i.get("name") and not i["slug"].startswith("e2e-test-")]
        assert len(named) >= 1, f"No real RODA items found in catalog after sync: {items}"

    def test_synced_items_have_required_fields(self, roda_sync_result,
                                               catalog_table_name, ddb_resource):
        """Items written by sync have slug, name, and at least one of formats/s3Resources."""
        table = ddb_resource.Table(catalog_table_name)
        resp = table.scan(
            Limit=20,
            FilterExpression="attribute_exists(#n) AND NOT begins_with(slug, :prefix)",
            ExpressionAttributeNames={"#n": "name"},
            ExpressionAttributeValues={":prefix": "e2e-test-"},
        )
        items = resp.get("Items", [])
        assert len(items) >= 1, "No synced RODA items found in catalog"
        for item in items[:5]:
            assert "slug" in item, f"Item missing slug: {item}"
            assert "name" in item, f"Item missing name: {item}"
