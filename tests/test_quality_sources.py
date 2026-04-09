"""
Unit tests for v0.13.0 features:
  - requires_transform dispatch in dataset-loader (#37)
  - Data quality metrics in preview handlers (#38)
  - Zenodo/Figshare research search (#39)
  - Federated search dispatch for zenodo/figshare (#39)

All external HTTP calls are mocked via unittest.mock.patch on urllib.request.urlopen.
"""

import importlib
import importlib.util
import json
import os
import sys
from unittest.mock import MagicMock, patch

REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_urlopen_response(body):
    """Create a mock urllib response context manager for a JSON body."""
    encoded = json.dumps(body).encode("utf-8")
    mock_resp = MagicMock()
    mock_resp.read.return_value = encoded
    mock_resp.__enter__ = lambda s: s
    mock_resp.__exit__ = MagicMock(return_value=False)
    return mock_resp


def _load_handler(lambda_dir: str, alias: str, env: dict | None = None):
    path = os.path.join(REPO_ROOT, "lambdas", lambda_dir, "handler.py")
    spec = importlib.util.spec_from_file_location(alias, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[alias] = mod
    with patch.dict(os.environ, env or {}):
        spec.loader.exec_module(mod)
    return mod


def _mock_ctx(tool_name: str = "target___test_tool"):
    ctx = MagicMock()
    ctx.client_context.custom = {"bedrockAgentCoreToolName": tool_name}
    return ctx


# ---------------------------------------------------------------------------
# Load modules
# ---------------------------------------------------------------------------

_research = _load_handler("research-search", "_research_handler")

# Dataset loader needs several env vars
_loader = _load_handler(
    "dataset-loader", "_loader_handler_qt",
    env={
        "TABLE_NAME": "test-catalog",
        "MANIFEST_BUCKET": "test-manifests",
        "QUICKSIGHT_ACCOUNT_ID": "123456789012",
    },
)


# ---------------------------------------------------------------------------
# TestRequiresTransform (#37)
# ---------------------------------------------------------------------------

class TestRequiresTransform:
    """Test that non-QuickSight-native formats return requires_transform."""

    def _make_catalog_item(self, slug: str, formats: list | None = None) -> dict:
        return {
            "slug": slug,
            "name": f"Test Dataset {slug}",
            "s3Resources": [{"arn": "arn:aws:s3:::test-bucket"}],
            "formats": formats or ["csv"],
            "requesterPays": False,
        }

    def test_nc_returns_requires_transform(self):
        item = self._make_catalog_item("test-nc", formats=["nc"])
        mock_table = MagicMock()
        mock_table.get_item.return_value = {"Item": item}

        with patch.object(_loader, "dynamodb") as mock_ddb:
            mock_ddb.Table.return_value = mock_table
            result = _loader.handler(
                {"slug": "test-nc", "prefix": "data/observations.nc"},
                _mock_ctx(),
            )

        assert result["status"] == "requires_transform"
        assert result["suggested_profile"] == "ingest-netcdf"
        assert result["format"] == "nc"
        assert "source_uri" in result

    def test_csv_loads_normally_not_requires_transform(self):
        """CSV is QuickSight-native so should NOT return requires_transform."""
        item = self._make_catalog_item("test-csv", formats=["csv"])
        mock_table = MagicMock()
        mock_table.get_item.return_value = {"Item": item}

        with patch.object(_loader, "dynamodb") as mock_ddb, \
             patch.object(_loader, "s3") as mock_s3, \
             patch.object(_loader, "quicksight") as mock_qs:
            mock_ddb.Table.return_value = mock_table
            # Simulate probe returning some CSV files
            paginator = MagicMock()
            paginator.paginate.return_value = [
                {"Contents": [{"Key": "data/file1.csv"}, {"Key": "data/file2.csv"}]}
            ]
            mock_s3.get_paginator.return_value = paginator
            mock_s3.put_object.return_value = {}

            # Mock QuickSight create calls
            mock_qs.create_data_source.return_value = {"Arn": "arn:aws:quicksight:us-east-1:123456789012:datasource/test"}
            mock_qs.describe_data_source.return_value = {"DataSource": {"Status": "CREATION_SUCCESSFUL"}}
            mock_qs.create_data_set.return_value = {}

            result = _loader.handler(
                {"slug": "test-csv", "prefix": "data/file.csv"},
                _mock_ctx(),
            )

        # CSV should not trigger requires_transform
        assert result.get("status") != "requires_transform"

    def test_pdf_returns_requires_transform(self):
        item = self._make_catalog_item("test-pdf", formats=["pdf"])
        mock_table = MagicMock()
        mock_table.get_item.return_value = {"Item": item}

        with patch.object(_loader, "dynamodb") as mock_ddb:
            mock_ddb.Table.return_value = mock_table
            result = _loader.handler(
                {"slug": "test-pdf", "prefix": "papers/document.pdf"},
                _mock_ctx(),
            )

        assert result["status"] == "requires_transform"
        assert result["suggested_profile"] == "ingest-pdf-extract"
        assert result["format"] == "pdf"


# ---------------------------------------------------------------------------
# TestQualityMetrics (#38)
# ---------------------------------------------------------------------------

class TestQualityMetrics:
    """Test data quality metrics computed from sample rows."""

    def test_s3_preview_includes_quality(self):
        """s3-preview response should include a quality block."""
        # Load s3-preview with env
        s3_preview = _load_handler(
            "s3-preview", "_s3_preview_qt",
            env={"SOURCES_CONFIG": json.dumps([{"label": "test", "bucket": "test-bucket", "prefix": "data/"}])},
        )

        mock_head = {"ContentLength": 100, "ContentType": "text/csv"}
        csv_bytes = b"name,age,city\nAlice,30,NYC\nBob,,Boston\n"

        with patch.object(s3_preview, "s3") as mock_s3:
            mock_s3.head_object.return_value = mock_head
            body_mock = MagicMock()
            body_mock.read.return_value = csv_bytes
            mock_s3.get_object.return_value = {"Body": body_mock}
            mock_s3.exceptions = MagicMock()
            mock_s3.exceptions.NoSuchKey = type("NoSuchKey", (Exception,), {})

            result = s3_preview.handler(
                {"source": "test", "key": "file.csv"},
                _mock_ctx(),
            )

        assert "quality" in result
        q = result["quality"]
        assert "row_count" in q
        assert "null_pct" in q
        assert "estimated_cardinality" in q
        assert "duplicate_row_pct" in q

    def test_null_pct_computed(self):
        """Rows with None values should yield correct null_pct."""
        s3_preview = sys.modules.get("_s3_preview_qt") or _load_handler(
            "s3-preview", "_s3_preview_qt2", env={"SOURCES_CONFIG": "[]"}
        )
        fn = s3_preview._compute_quality

        rows = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": None},
            {"name": "", "age": 25},
        ]
        q = fn(rows)
        # name: 1 empty string out of 3 = 33.3%
        assert q["null_pct"]["name"] == 33.3
        # age: 1 None out of 3 = 33.3%
        assert q["null_pct"]["age"] == 33.3

    def test_cardinality_computed(self):
        """Unique value counts should be correct."""
        s3_preview = sys.modules.get("_s3_preview_qt") or _load_handler(
            "s3-preview", "_s3_preview_qt3", env={"SOURCES_CONFIG": "[]"}
        )
        fn = s3_preview._compute_quality

        rows = [
            {"color": "red", "size": 1},
            {"color": "blue", "size": 1},
            {"color": "red", "size": 2},
        ]
        q = fn(rows)
        assert q["estimated_cardinality"]["color"] == 2
        assert q["estimated_cardinality"]["size"] == 2

    def test_duplicate_detection(self):
        """Duplicate rows should be detected."""
        s3_preview = sys.modules.get("_s3_preview_qt") or _load_handler(
            "s3-preview", "_s3_preview_qt4", env={"SOURCES_CONFIG": "[]"}
        )
        fn = s3_preview._compute_quality

        rows = [
            {"a": 1, "b": 2},
            {"a": 1, "b": 2},
            {"a": 3, "b": 4},
        ]
        q = fn(rows)
        assert q["duplicate_row_pct"] > 0
        # 1 duplicate out of 3 = 33.3%
        assert q["duplicate_row_pct"] == 33.3

    def test_empty_rows_quality(self):
        """Empty rows should return zeroed quality."""
        s3_preview = sys.modules.get("_s3_preview_qt") or _load_handler(
            "s3-preview", "_s3_preview_qt5", env={"SOURCES_CONFIG": "[]"}
        )
        fn = s3_preview._compute_quality

        q = fn([])
        assert q["row_count"] == 0
        assert q["null_pct"] == {}
        assert q["estimated_cardinality"] == {}
        assert q["duplicate_row_pct"] == 0.0

    def test_snowflake_preview_includes_quality(self):
        """Snowflake preview response should include a quality block."""
        sf_preview = _load_handler(
            "snowflake-preview", "_sf_preview_qt",
            env={"SNOWFLAKE_SECRET_ARN": "arn:aws:secretsmanager:us-east-1:123456789012:secret:test"},
        )

        mock_config = {
            "account": "test_account",
            "user": "test_user",
            "password": "test_pass",
            "warehouse": "wh",
            "role": "role",
            "database": "db",
        }
        sf_result = {
            "resultSetMetaData": {"rowType": [{"name": "id"}, {"name": "val"}]},
            "data": [["1", "a"], ["2", "b"], ["2", "b"]],
        }

        with patch.object(sf_preview, "secrets_client") as mock_secrets, \
             patch.object(sf_preview, "_snowflake_execute", return_value=sf_result):
            mock_secrets.get_secret_value.return_value = {"SecretString": json.dumps(mock_config)}
            result = sf_preview.handler(
                {"source_id": "sf-test", "schema": "public", "table": "users"},
                _mock_ctx(),
            )

        assert "quality" in result
        q = result["quality"]
        assert q["row_count"] == 3
        assert q["duplicate_row_pct"] > 0


# ---------------------------------------------------------------------------
# TestResearchSearch (#39)
# ---------------------------------------------------------------------------

_ZENODO_RESPONSE = {
    "hits": {
        "hits": [
            {
                "id": 12345,
                "metadata": {
                    "title": "Climate Data Archive 2025",
                    "description": "Global temperature and precipitation records",
                    "doi": "10.5281/zenodo.12345",
                    "publication_date": "2025-06-15",
                },
                "links": {"self": "https://zenodo.org/api/records/12345"},
                "files": [{"links": {"self": "https://zenodo.org/api/files/abc/data.csv"}}],
            },
        ]
    }
}

_FIGSHARE_RESPONSE = [
    {
        "id": 67890,
        "title": "Genomics Dataset for RNA Analysis",
        "description": "Single-cell RNA-seq dataset from human tissue samples",
        "doi": "10.6084/m9.figshare.67890",
        "published_date": "2025-08-01",
        "url_public_html": "https://figshare.com/articles/67890",
    },
]


class TestResearchSearch:
    def test_zenodo_happy_path(self):
        with patch("urllib.request.urlopen", return_value=_make_urlopen_response(_ZENODO_RESPONSE)):
            result = _research.handler({"query": "climate data"}, _mock_ctx())
        assert result["total"] >= 1
        r = result["results"][0]
        assert r["source_type"] == "zenodo"
        assert r["source_id"] == "zenodo/12345"
        assert "Climate Data" in r["display_name"]
        assert r["doi"] == "10.5281/zenodo.12345"

    def test_figshare_happy_path(self):
        with patch("urllib.request.urlopen", return_value=_make_urlopen_response(_FIGSHARE_RESPONSE)):
            result = _research.handler(
                {"query": "RNA genomics", "sources": ["figshare"]},
                _mock_ctx(),
            )
        assert result["total"] >= 1
        r = result["results"][0]
        assert r["source_type"] == "figshare"
        assert r["source_id"] == "figshare/67890"
        assert "Genomics" in r["display_name"]

    def test_rate_limit_retry(self):
        """429 should trigger retry and eventually succeed."""
        call_count = {"n": 0}

        def side_effect(req, timeout=None):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise urllib.error.HTTPError(
                    url="https://zenodo.org", code=429,
                    msg="Too Many Requests", hdrs={}, fp=None,
                )
            return _make_urlopen_response(_ZENODO_RESPONSE)

        import urllib.error
        with patch("urllib.request.urlopen", side_effect=side_effect):
            result = _research.handler(
                {"query": "climate", "sources": ["zenodo"]},
                _mock_ctx(),
            )
        assert result["total"] >= 1
        assert call_count["n"] >= 2

    def test_empty_query_returns_error(self):
        result = _research.handler({}, _mock_ctx())
        assert "error" in result

    def test_max_results_capped(self):
        """max_results > 50 should be capped at 50."""
        big_response = {"hits": {"hits": [
            {"id": i, "metadata": {"title": f"Dataset {i}", "description": "test", "doi": "", "publication_date": ""},
             "links": {"self": ""}, "files": []}
            for i in range(60)
        ]}}
        with patch("urllib.request.urlopen", return_value=_make_urlopen_response(big_response)):
            result = _research.handler(
                {"query": "test", "sources": ["zenodo"], "max_results": 100},
                _mock_ctx(),
            )
        assert len(result["results"]) <= 50

    def test_federated_search_dispatches_zenodo(self):
        """Verify federated search has zenodo in its dispatch dict."""
        fed = _load_handler(
            "federated-search", "_fed_handler_qt",
            env={"REGISTRY_TABLE": "qs-data-source-registry", "CATALOG_TABLE": "qs-catalog"},
        )
        # Check that _search_fn dict (inside handler closure) includes zenodo
        assert hasattr(fed, "_search_zenodo")
        assert hasattr(fed, "_search_figshare")

        # Also verify dispatch works end-to-end
        source = {
            "source_id": "zenodo-main",
            "type": "zenodo",
            "display_name": "Zenodo",
            "description": "Zenodo research data",
            "data_classification": "public",
        }
        mock_hit = {
            "source_id": "zenodo-main", "source_type": "zenodo",
            "display_name": "Climate Data", "match_score": 0.8,
            "description": "temperature records", "quality_score": None,
        }
        mock_reg = MagicMock()
        mock_reg.scan.return_value = {"Items": [source]}
        with patch.object(fed, "dynamodb") as mock_ddb, \
             patch.object(fed, "_search_zenodo", return_value=[mock_hit]):
            mock_ddb.Table.return_value = mock_reg
            result = fed.handler({"query": "climate data", "caller_clearance": "public"}, _mock_ctx())
        assert result["total"] == 1
        assert result["results"][0]["source_type"] == "zenodo"
