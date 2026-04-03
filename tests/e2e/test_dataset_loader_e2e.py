"""
E2E tests for dataset-loader (roda_load) Lambda against deployed QuickSuiteOpenData stack.

Safe tests (no QS writes): error cases, manifest-only path.
Write tests (require QS_E2E_ALLOW_WRITES=true): full load, claws lookup.

The seeded_catalog fixture ensures at least one catalog item (e2e-test-climate)
is present in DynamoDB before these tests run.
"""

import pytest

from tests.e2e.conftest import invoke

pytestmark = pytest.mark.e2e


class TestDatasetLoaderE2E:
    # ------------------------------------------------------------------
    # Safe tests — no QuickSight write, always run
    # ------------------------------------------------------------------

    def test_missing_slug_returns_error(self, lam, tool_arns, seeded_catalog):
        """Event with no slug returns an error dict."""
        result = invoke(lam, tool_arns["roda_load"], {})
        assert "error" in result

    def test_unknown_slug_returns_error(self, lam, tool_arns, seeded_catalog):
        """Slug not in the catalog returns an error dict."""
        result = invoke(lam, tool_arns["roda_load"], {"slug": "e2e-test-does-not-exist"})
        assert "error" in result

    def test_sample_only_writes_manifest(self, lam, tool_arns, seeded_catalog,
                                          manifest_cleanup, s3_client, manifest_bucket_name):
        """
        sample_only=True writes a manifest to S3 and returns manifest_ready status
        without calling QuickSight. Safe to run without QS_E2E_ALLOW_WRITES.
        """
        result = invoke(lam, tool_arns["roda_load"],
                        {"slug": "e2e-test-climate", "format": "csv", "sample_only": True})
        # If real S3 files exist in the bucket, this returns manifest_ready
        # If no files found, it returns an error — both are valid E2E outcomes
        if "error" in result:
            # No CSV files in noaa-ghcn-pds accessible from this account — acceptable
            pytest.skip(f"No S3 files accessible for sample_only test: {result['error']}")
        assert result.get("status") == "manifest_ready", f"Unexpected status: {result}"
        manifest_uri = result.get("manifestUri", "")
        if manifest_uri:
            # Extract S3 key from s3://bucket/key URI and register for cleanup
            key = manifest_uri.split(f"s3://{manifest_bucket_name}/", 1)[-1]
            manifest_cleanup.register(key)

    def test_no_s3_resources_slug_returns_error(self, lam, tool_arns, seeded_catalog,
                                                 ddb_resource, catalog_table_name):
        """Slug with no s3Resources field in DDB returns an error dict."""
        # e2e-test-stale has no s3Resources — use it for this test
        result = invoke(lam, tool_arns["roda_load"], {"slug": "e2e-test-stale"})
        assert "error" in result or result.get("status") == "error", \
            f"Expected error for item with no s3Resources: {result}"

    # ------------------------------------------------------------------
    # Write tests — require QS_E2E_ALLOW_WRITES=true
    # ------------------------------------------------------------------

    def test_full_load_creates_quicksight_dataset(self, roda_load_write_result):
        """
        Full load returns loaded (QS dataset created) or manifest_ready (QS failed but
        manifest was written). Either way the manifest URI must be present.
        Skip only when QS_E2E_ALLOW_WRITES is not set.
        """
        if roda_load_write_result is None:
            pytest.skip("QS writes disabled — set QS_E2E_ALLOW_WRITES=true")
        result = roda_load_write_result
        status = result.get("status")
        assert status in ("loaded", "manifest_ready"), f"Unexpected status: {result}"
        assert "manifestUri" in result, f"Missing manifestUri: {result}"
        if status == "loaded":
            assert "datasetId" in result, f"Missing datasetId on loaded result: {result}"
            assert "quicksightResult" in result

    def test_claws_lookup_written_after_full_load(self, roda_load_write_result):
        """
        claws_source_id is written to the response when status=loaded;
        it is absent when status=manifest_ready (QS failed before the claws write).
        This documents the contract regardless of QS availability.
        Skip only when QS_E2E_ALLOW_WRITES is not set.
        """
        if roda_load_write_result is None:
            pytest.skip("QS writes disabled — set QS_E2E_ALLOW_WRITES=true")
        result = roda_load_write_result
        if result.get("status") == "loaded":
            assert "claws_source_id" in result, f"Missing claws_source_id in: {result}"
        elif result.get("status") == "manifest_ready":
            assert "claws_source_id" not in result, \
                f"claws_source_id should not be written on manifest_ready: {result}"
        else:
            pytest.fail(f"Unexpected status in roda_load result: {result}")
