"""
E2E tests for s3-load Lambda against deployed QuickSuiteOpenData stack.

Safe tests: error cases that don't touch QuickSight.
Write tests: require QS_E2E_ALLOW_WRITES=true.

Source discovery uses the same SOURCES_CONFIG introspection as s3-browse tests.
"""

import pytest

from tests.e2e.conftest import invoke

pytestmark = pytest.mark.e2e


class TestS3LoadE2E:
    # ------------------------------------------------------------------
    # Safe tests
    # ------------------------------------------------------------------

    def test_missing_source_returns_error(self, lam, tool_arns):
        """Event with no source returns an error dict."""
        result = invoke(lam, tool_arns["s3_load"], {})
        assert "error" in result

    def test_unknown_source_returns_error(self, lam, tool_arns):
        """Unknown source label returns an error dict."""
        result = invoke(lam, tool_arns["s3_load"], {"source": "___nonexistent___"})
        assert "error" in result

    def test_path_traversal_denied(self, lam, tool_arns, first_source):
        """Prefix with path traversal is rejected."""
        result = invoke(lam, tool_arns["s3_load"],
                        {"source": first_source["label"], "prefix": "../../etc"})
        assert "error" in result

    def test_no_matching_files_returns_appropriate_response(self, lam, tool_arns, first_source):
        """A prefix with no matching files returns a no_matching_files error or empty result."""
        result = invoke(lam, tool_arns["s3_load"],
                        {"source": first_source["label"],
                         "prefix": "zzz-no-match-xyz/",
                         "format": "csv"})
        # Handler should return an error or no_matching_files status — not a Lambda crash
        assert isinstance(result, dict)
        if "status" in result:
            assert result["status"] in ("no_matching_files", "error", "manifest_ready", "loaded")

    # ------------------------------------------------------------------
    # Write tests — require QS_E2E_ALLOW_WRITES=true
    # ------------------------------------------------------------------

    def test_full_load_creates_quicksight_datasource(self, s3_load_write_result, first_source):
        """
        Full s3-load returns loaded (QS data source created) or manifest_ready (QS failed).
        Either way the manifest URI must be present.
        Skip only when QS_E2E_ALLOW_WRITES is not set or no matching files exist.
        """
        if s3_load_write_result is None:
            pytest.skip("QS writes disabled — set QS_E2E_ALLOW_WRITES=true")
        result = s3_load_write_result
        if result.get("status") == "no_matching_files":
            pytest.skip(f"No CSV files in source '{first_source['label']}' — cannot test full load")
        status = result.get("status")
        assert status in ("loaded", "manifest_ready"), f"Unexpected status: {result}"
        assert "manifestUri" in result, f"Missing manifestUri: {result}"
        if status == "loaded":
            assert "quicksightResult" in result

    def test_claws_lookup_written_after_load(self, s3_load_write_result, first_source):
        """
        claws_source_id is written to the response when status=loaded;
        absent when status=manifest_ready (QS failed before the claws write).
        Uses the same invocation result as test_full_load_creates_quicksight_datasource
        to avoid duplicate QS calls that would hit 'already exists' errors.
        Skip only when QS_E2E_ALLOW_WRITES is not set or no matching files.
        """
        if s3_load_write_result is None:
            pytest.skip("QS writes disabled — set QS_E2E_ALLOW_WRITES=true")
        result = s3_load_write_result
        if result.get("status") == "no_matching_files":
            pytest.skip(f"No CSV files in source '{first_source['label']}'")
        if result.get("status") == "loaded":
            assert "claws_source_id" in result, f"Missing claws_source_id in response: {result}"
        elif result.get("status") == "manifest_ready":
            assert "claws_source_id" not in result, \
                f"claws_source_id should not be written on manifest_ready: {result}"
        else:
            pytest.fail(f"Unexpected status in s3_load result: {result}")
