"""
Integration tests for redshift-browse/handler.py and redshift-preview/handler.py.

Uses Substrate (real AWS emulator) for Secrets Manager and the Redshift Data API
(ExecuteStatement / DescribeStatement / GetStatementResult) introduced in Substrate
v0.50.0 (scttfrdmn/substrate#268).

Limitation: Substrate's redshift-data plugin always returns FINISHED status and
returns empty Records unless results are seeded at Go plugin init time (no HTTP
seeding endpoint exists yet).  Tests that require FAILED/ABORTED status control or
specific result rows are tracked in scttfrdmn/substrate#<TODO: seed endpoint issue>
and use unittest.mock in the meantime.
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

_SECRET_NAME = "test/redshift-creds"
_REDSHIFT_SECRET = {
    "workgroup": "my-workgroup",
    "database": "mydb",
    "secret_arn": "arn:aws:secretsmanager:us-east-1:123:secret:redshift-inner-creds",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _seed_secret(substrate_url):
    sm = boto3.client("secretsmanager", endpoint_url=substrate_url, region_name="us-east-1")
    sm.create_secret(Name=_SECRET_NAME, SecretString=json.dumps(_REDSHIFT_SECRET))
    return f"arn:aws:secretsmanager:us-east-1:123456789012:secret:{_SECRET_NAME}"


def _reload_browse(substrate_url, monkeypatch, secret_arn):
    monkeypatch.setenv("AWS_ENDPOINT_URL", substrate_url)
    monkeypatch.setenv("REDSHIFT_SECRET_ARN", secret_arn)
    path = os.path.join(REPO_ROOT, "lambdas", "redshift-browse", "handler.py")
    alias = "_rs_browse_integ"
    spec = importlib.util.spec_from_file_location(alias, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[alias] = mod
    spec.loader.exec_module(mod)
    return mod


def _reload_preview(substrate_url, monkeypatch, secret_arn):
    monkeypatch.setenv("AWS_ENDPOINT_URL", substrate_url)
    monkeypatch.setenv("REDSHIFT_SECRET_ARN", secret_arn)
    path = os.path.join(REPO_ROOT, "lambdas", "redshift-preview", "handler.py")
    alias = "_rs_preview_integ"
    spec = importlib.util.spec_from_file_location(alias, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[alias] = mod
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# redshift-browse — Substrate integration tests
# ---------------------------------------------------------------------------

class TestRedshiftBrowse:
    """Integration tests using real Substrate Secrets Manager + Redshift Data API."""

    @pytest.mark.integration
    def test_happy_path_returns_valid_structure(self, substrate_url, reset_substrate, monkeypatch):
        """Handler completes the full SM → execute → describe → get_result round-trip.

        Substrate redshift-data returns empty Records (no HTTP seeding yet), so count=0.
        The test verifies response structure and correct config extraction from SM.
        """
        secret_arn = _seed_secret(substrate_url)
        mod = _reload_browse(substrate_url, monkeypatch, secret_arn)

        result = mod.handler({"source_id": "rs-prod"}, None)

        assert "error" not in result
        assert result["source_id"] == "rs-prod"
        assert result["workgroup"] == "my-workgroup"
        assert result["database"] == "mydb"
        assert isinstance(result["tables"], list)
        assert isinstance(result["count"], int)

    @pytest.mark.integration
    def test_source_id_required(self, substrate_url, reset_substrate, monkeypatch):
        secret_arn = _seed_secret(substrate_url)
        mod = _reload_browse(substrate_url, monkeypatch, secret_arn)
        result = mod.handler({}, None)
        assert "error" in result

    @pytest.mark.integration
    def test_not_configured_when_no_secret_arn(self, substrate_url, reset_substrate, monkeypatch):
        monkeypatch.setenv("AWS_ENDPOINT_URL", substrate_url)
        monkeypatch.setenv("REDSHIFT_SECRET_ARN", "")
        path = os.path.join(REPO_ROOT, "lambdas", "redshift-browse", "handler.py")
        alias = "_rs_browse_no_secret"
        spec = importlib.util.spec_from_file_location(alias, path)
        mod = importlib.util.module_from_spec(spec)
        sys.modules[alias] = mod
        spec.loader.exec_module(mod)
        result = mod.handler({"source_id": "rs-no-config"}, None)
        assert "error" in result
        assert "not configured" in result["error"]


# ---------------------------------------------------------------------------
# redshift-browse — error-state tests (MagicMock; pending Substrate status control)
# Tracked: scttfrdmn/substrate — HTTP status override for redshift-data plugin.
# ---------------------------------------------------------------------------

_STATEMENT_ID = "abc-123-stmt"


def _make_browse():
    path = os.path.join(REPO_ROOT, "lambdas", "redshift-browse", "handler.py")
    alias = "_rs_browse_mock"
    spec = importlib.util.spec_from_file_location(alias, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[alias] = mod
    with patch.dict(os.environ, {"REDSHIFT_SECRET_ARN": "arn:test"}):
        spec.loader.exec_module(mod)
    return mod


def _make_preview():
    path = os.path.join(REPO_ROOT, "lambdas", "redshift-preview", "handler.py")
    alias = "_rs_preview_mock"
    spec = importlib.util.spec_from_file_location(alias, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[alias] = mod
    with patch.dict(os.environ, {"REDSHIFT_SECRET_ARN": "arn:test"}):
        spec.loader.exec_module(mod)
    return mod


_rs_browse_mock = _make_browse()
_rs_preview_mock = _make_preview()


def _mock_clients(mod, records, column_metadata=None, final_status="FINISHED"):
    mock_sc = MagicMock()
    mock_sc.get_secret_value.return_value = {"SecretString": json.dumps(_REDSHIFT_SECRET)}
    mock_rd = MagicMock()
    mock_rd.execute_statement.return_value = {"Id": _STATEMENT_ID}
    mock_rd.describe_statement.return_value = {"Status": final_status, "Error": ""}
    result_resp = {"Records": records}
    if column_metadata is not None:
        result_resp["ColumnMetadata"] = column_metadata
    mock_rd.get_statement_result.return_value = result_resp
    return (
        patch.object(mod, "secrets_client", mock_sc),
        patch.object(mod, "redshift_data", mock_rd),
    )


_TABLES_RECORDS = [
    [{"stringValue": "public"}, {"stringValue": "orders"}, {"stringValue": "BASE TABLE"}],
    [{"stringValue": "public"}, {"stringValue": "customers"}, {"stringValue": "BASE TABLE"}],
    [{"stringValue": "analytics"}, {"stringValue": "summary"}, {"stringValue": "BASE TABLE"}],
]

_PREVIEW_COLUMN_METADATA = [{"name": "id"}, {"name": "name"}, {"name": "amount"}]
_PREVIEW_RECORDS = [
    [{"stringValue": "1"}, {"stringValue": "Alice"}, {"stringValue": "99.99"}],
    [{"stringValue": "2"}, {"stringValue": "Bob"}, {"stringValue": "49.50"}],
]


class TestRedshiftBrowseResultParsing:
    """Tests requiring specific result rows or non-FINISHED status.

    Uses MagicMock until Substrate gains an HTTP seeding/status-override endpoint.
    """

    def test_happy_path_parses_rows_correctly(self):
        p_sc, p_rd = _mock_clients(_rs_browse_mock, _TABLES_RECORDS)
        with p_sc, p_rd:
            result = _rs_browse_mock.handler({"source_id": "rs-prod"}, None)
        assert result["count"] == 3
        assert result["tables"][0] == {"schema": "public", "name": "orders", "type": "BASE TABLE"}

    def test_query_times_out_returns_error(self):
        mock_sc = MagicMock()
        mock_sc.get_secret_value.return_value = {"SecretString": json.dumps(_REDSHIFT_SECRET)}
        mock_rd = MagicMock()
        mock_rd.execute_statement.return_value = {"Id": _STATEMENT_ID}
        mock_rd.describe_statement.return_value = {"Status": "STARTED"}
        with patch.object(_rs_browse_mock, "secrets_client", mock_sc):
            with patch.object(_rs_browse_mock, "redshift_data", mock_rd):
                with patch.object(_rs_browse_mock, "_POLL_MAX", 2):
                    with patch("time.sleep"):
                        result = _rs_browse_mock.handler({"source_id": "rs-slow"}, None)
        assert "error" in result
        assert "timed out" in result["error"]

    def test_failed_status_returns_error(self):
        mock_sc = MagicMock()
        mock_sc.get_secret_value.return_value = {"SecretString": json.dumps(_REDSHIFT_SECRET)}
        mock_rd = MagicMock()
        mock_rd.execute_statement.return_value = {"Id": _STATEMENT_ID}
        mock_rd.describe_statement.return_value = {
            "Status": "FAILED",
            "Error": "Permission denied on table information_schema.tables",
        }
        with patch.object(_rs_browse_mock, "secrets_client", mock_sc):
            with patch.object(_rs_browse_mock, "redshift_data", mock_rd):
                with patch("time.sleep"):
                    result = _rs_browse_mock.handler({"source_id": "rs-fail"}, None)
        assert "error" in result
        assert "Redshift query failed" in result["error"]
        assert "Permission denied" in result["error"]


# ---------------------------------------------------------------------------
# redshift-preview — Substrate integration tests
# ---------------------------------------------------------------------------

class TestRedshiftPreview:
    """Integration tests using real Substrate Secrets Manager + Redshift Data API."""

    @pytest.mark.integration
    def test_happy_path_returns_valid_structure(self, substrate_url, reset_substrate, monkeypatch):
        """Full SM → execute → describe → get_result round-trip.

        Substrate returns empty Records; test verifies response shape and field presence.
        """
        secret_arn = _seed_secret(substrate_url)
        mod = _reload_preview(substrate_url, monkeypatch, secret_arn)

        result = mod.handler(
            {"source_id": "rs-prod", "schema": "public", "table": "orders"}, None
        )

        assert "error" not in result
        assert result["source_id"] == "rs-prod"
        assert result["schema"] == "public"
        assert result["table"] == "orders"
        assert isinstance(result["columns"], list)
        assert isinstance(result["sample_rows"], list)
        assert result["format"] == "redshift"

    @pytest.mark.integration
    def test_invalid_table_name_returns_error(self, substrate_url, reset_substrate, monkeypatch):
        secret_arn = _seed_secret(substrate_url)
        mod = _reload_preview(substrate_url, monkeypatch, secret_arn)
        result = mod.handler(
            {"source_id": "rs-prod", "schema": "public", "table": "orders; DROP TABLE foo--"},
            None,
        )
        assert "error" in result
        assert "invalid table name" in result["error"]

    @pytest.mark.integration
    def test_invalid_schema_name_returns_error(self, substrate_url, reset_substrate, monkeypatch):
        secret_arn = _seed_secret(substrate_url)
        mod = _reload_preview(substrate_url, monkeypatch, secret_arn)
        result = mod.handler(
            {"source_id": "rs-prod", "schema": "public'; DELETE FROM x--", "table": "orders"},
            None,
        )
        assert "error" in result
        assert "invalid table name" in result["error"]

    @pytest.mark.integration
    def test_source_id_required(self, substrate_url, reset_substrate, monkeypatch):
        secret_arn = _seed_secret(substrate_url)
        mod = _reload_preview(substrate_url, monkeypatch, secret_arn)
        result = mod.handler({"schema": "public", "table": "orders"}, None)
        assert "error" in result

    @pytest.mark.integration
    def test_schema_required(self, substrate_url, reset_substrate, monkeypatch):
        secret_arn = _seed_secret(substrate_url)
        mod = _reload_preview(substrate_url, monkeypatch, secret_arn)
        result = mod.handler({"source_id": "rs-prod", "table": "orders"}, None)
        assert "error" in result

    @pytest.mark.integration
    def test_table_required(self, substrate_url, reset_substrate, monkeypatch):
        secret_arn = _seed_secret(substrate_url)
        mod = _reload_preview(substrate_url, monkeypatch, secret_arn)
        result = mod.handler({"source_id": "rs-prod", "schema": "public"}, None)
        assert "error" in result


# ---------------------------------------------------------------------------
# redshift-preview — result-parsing and error-state tests (MagicMock)
# ---------------------------------------------------------------------------

class TestRedshiftPreviewResultParsing:
    """Tests requiring specific row data or non-FINISHED status. Uses MagicMock."""

    def test_happy_path_parses_rows_and_columns(self):
        p_sc, p_rd = _mock_clients(_rs_preview_mock, _PREVIEW_RECORDS, _PREVIEW_COLUMN_METADATA)
        with p_sc, p_rd:
            result = _rs_preview_mock.handler(
                {"source_id": "rs-prod", "schema": "public", "table": "orders"}, None
            )
        assert result["columns"] == ["id", "name", "amount"]
        assert len(result["sample_rows"]) == 2
        assert result["sample_rows"][0]["name"] == "Alice"
        assert result["format"] == "redshift"

    def test_max_rows_capped_at_25(self):
        rows = [[{"stringValue": str(i)}, {"stringValue": f"n{i}"}] for i in range(25)]
        mock_sc = MagicMock()
        mock_sc.get_secret_value.return_value = {"SecretString": json.dumps(_REDSHIFT_SECRET)}
        mock_rd = MagicMock()
        mock_rd.execute_statement.return_value = {"Id": _STATEMENT_ID}
        mock_rd.describe_statement.return_value = {"Status": "FINISHED"}
        mock_rd.get_statement_result.return_value = {
            "ColumnMetadata": [{"name": "id"}, {"name": "name"}],
            "Records": rows,
        }
        with patch.object(_rs_preview_mock, "secrets_client", mock_sc):
            with patch.object(_rs_preview_mock, "redshift_data", mock_rd):
                with patch("time.sleep"):
                    result = _rs_preview_mock.handler(
                        {"source_id": "rs-prod", "schema": "public", "table": "orders",
                         "max_rows": 100},
                        None,
                    )
        call_kwargs = mock_rd.execute_statement.call_args[1]
        assert "LIMIT 25" in call_kwargs["Sql"]
        assert result["row_count"] <= 25

    def test_aborted_status_returns_error(self):
        mock_sc = MagicMock()
        mock_sc.get_secret_value.return_value = {"SecretString": json.dumps(_REDSHIFT_SECRET)}
        mock_rd = MagicMock()
        mock_rd.execute_statement.return_value = {"Id": _STATEMENT_ID}
        mock_rd.describe_statement.return_value = {"Status": "ABORTED", "Error": "Query aborted"}
        with patch.object(_rs_preview_mock, "secrets_client", mock_sc):
            with patch.object(_rs_preview_mock, "redshift_data", mock_rd):
                with patch("time.sleep"):
                    result = _rs_preview_mock.handler(
                        {"source_id": "rs-prod", "schema": "public", "table": "orders"}, None
                    )
        assert "error" in result
        assert "Redshift query failed" in result["error"]
