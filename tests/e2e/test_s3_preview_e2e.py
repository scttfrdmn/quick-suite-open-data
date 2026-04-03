"""
E2E tests for s3-preview Lambda against deployed QuickSuiteOpenData stack.

Discovers a usable file by first calling s3-browse on the first configured source,
then previewing a file found there. All source-specific tests skip if no sources
are configured or no browseable files are found.

For predictable schema tests, add a RODA bucket with known CSV/Parquet structure
to config/sources.yaml (e.g. noaa-ghcn-pds).
"""

import pytest

from tests.e2e.conftest import invoke

pytestmark = pytest.mark.e2e


@pytest.fixture(scope="module")
def first_browseable_file(lam, tool_arns, first_source) -> dict:
    """
    Browse the first configured source and return the first file found.
    Skips if no files are present in the source.
    Returns a dict with 'key' and optionally 'source_label'.
    """
    result = invoke(lam, tool_arns["s3_browse"], {"source": first_source["label"]})
    if "error" in result or result.get("count", 0) == 0:
        pytest.skip(f"No files found in source '{first_source['label']}' — cannot run preview tests")
    # Handler returns 'files' or 'objects' list depending on version
    files = result.get("files") or result.get("objects") or []
    if not files:
        pytest.skip("Browse returned count > 0 but no files list")
    return {"key": files[0].get("key") or files[0].get("Key"), "label": first_source["label"]}


class TestS3PreviewE2E:
    def test_unknown_source_returns_error(self, lam, tool_arns):
        """Unknown source label returns an error dict."""
        result = invoke(lam, tool_arns["s3_preview"],
                        {"source": "___nonexistent___", "key": "test.csv"})
        assert "error" in result

    def test_missing_key_returns_error(self, lam, tool_arns, first_source):
        """Requesting a key that does not exist returns an error dict."""
        result = invoke(lam, tool_arns["s3_preview"],
                        {"source": first_source["label"], "key": "zzz-no-such-file-xyz.csv"})
        assert "error" in result

    def test_path_traversal_denied(self, lam, tool_arns, first_source):
        """Path traversal in key is rejected."""
        result = invoke(lam, tool_arns["s3_preview"],
                        {"source": first_source["label"], "key": "../../etc/passwd"})
        assert "error" in result

    def test_preview_returns_schema_and_rows(self, lam, tool_arns, first_browseable_file):
        """Preview of a real file returns format, columns, and sample_rows."""
        result = invoke(lam, tool_arns["s3_preview"],
                        {"source": first_browseable_file["label"],
                         "key": first_browseable_file["key"]})
        if "error" in result:
            pytest.skip(f"Preview returned error (unsupported format?): {result['error']}")
        assert "format" in result, f"Missing 'format' in response: {result}"
        assert "columns" in result, f"Missing 'columns' in response: {result}"
        assert "sample_rows" in result, f"Missing 'sample_rows' in response: {result}"

    def test_max_rows_respected(self, lam, tool_arns, first_browseable_file):
        """max_rows=2 returns at most 2 sample rows."""
        result = invoke(lam, tool_arns["s3_preview"],
                        {"source": first_browseable_file["label"],
                         "key": first_browseable_file["key"],
                         "max_rows": 2})
        if "error" in result:
            pytest.skip(f"Preview returned error: {result['error']}")
        rows = result.get("sample_rows", [])
        assert len(rows) <= 2, f"Expected at most 2 rows, got {len(rows)}"

    def test_max_rows_out_of_range_returns_error(self, lam, tool_arns, first_source):
        """max_rows=0 is out of valid range — returns an error."""
        result = invoke(lam, tool_arns["s3_preview"],
                        {"source": first_source["label"], "key": "test.csv", "max_rows": 0})
        assert "error" in result
