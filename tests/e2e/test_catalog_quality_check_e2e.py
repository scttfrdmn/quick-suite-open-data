"""
E2E tests for catalog-quality-check Lambda against deployed QuickSuiteOpenData stack.

Uses the session-scoped quality_check_result fixture — the Lambda is invoked once per
session (after seeded_catalog ensures e2e-test-stale is present). DDB read assertions
run against the state written by that single invocation.
"""

import pytest

from tests.e2e.conftest import E2E_SLUG_PREFIX

pytestmark = pytest.mark.e2e


class TestCatalogQualityCheckE2E:
    def test_handler_returns_scan_counts(self, quality_check_result, seeded_catalog):
        """Quality-check returns scanned, stale_count, unreachable_count."""
        result = quality_check_result
        for key in ("scanned", "stale_count", "unreachable_count"):
            assert key in result, f"Missing key '{key}' in response: {result}"
        assert result["scanned"] >= len(seeded_catalog)

    def test_stale_item_flagged(self, quality_check_result, catalog_table_name, ddb_resource):
        """e2e-test-stale (no last_updated) is marked stale=True after quality check."""
        table = ddb_resource.Table(catalog_table_name)
        item = table.get_item(Key={"slug": "e2e-test-stale"})["Item"]
        assert item.get("stale") is True, f"Expected stale=True on e2e-test-stale: {item}"

    def test_fresh_item_not_stale(self, quality_check_result, catalog_table_name, ddb_resource):
        """e2e-test-climate (recent last_updated) is not marked stale."""
        table = ddb_resource.Table(catalog_table_name)
        item = table.get_item(Key={"slug": "e2e-test-climate"})["Item"]
        assert "stale" not in item or item.get("stale") is not True, \
            f"e2e-test-climate should not be stale: {item}"

    def test_last_verified_written_to_fresh_item(self, quality_check_result,
                                                  catalog_table_name, ddb_resource):
        """quality-check writes last_verified to non-stale items too."""
        table = ddb_resource.Table(catalog_table_name)
        item = table.get_item(Key={"slug": "e2e-test-climate"})["Item"]
        assert "last_verified" in item, f"Missing last_verified on e2e-test-climate: {item}"

    def test_quality_score_written(self, quality_check_result, catalog_table_name, ddb_resource):
        """quality-check writes quality_score dict to each scanned item."""
        table = ddb_resource.Table(catalog_table_name)
        item = table.get_item(Key={"slug": "e2e-test-climate"})["Item"]
        qs = item.get("quality_score")
        assert qs is not None, f"Missing quality_score on e2e-test-climate: {item}"
        assert "freshness" in qs

    def test_stale_count_at_least_one(self, quality_check_result):
        """Stale count is at least 1 because e2e-test-stale has no last_updated."""
        result = quality_check_result
        assert result["stale_count"] >= 1, \
            f"Expected stale_count >= 1 (e2e-test-stale has no last_updated): {result}"
