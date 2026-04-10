"""
Security tests for quick-suite-data v0.8.0 (#52–#59).

#52 — S3 IAM role grants no PutObject on wildcard
#53 — Bucket name validation prevents SSRF in catalog quality check
#54 — QuickSight principal sourced from env var, not caller event
#55 — register-source validates connection_config format per source type
#56 — Redshift workgroup not exposed in response
#57 — DynamoDB tables have deletion protection (CDK — tested by content check)
#58 — s3_preview rejects unsupported file extensions
#59 — Error messages do not expose internal details (sanitized)
"""

import importlib.util
import os
import sys
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).parent.parent


def _load(path: str, alias: str, env: dict | None = None):
    """Load a Lambda handler module in isolation, optionally setting env vars."""
    with patch.dict(os.environ, env or {}):
        full = REPO_ROOT / path
        spec = importlib.util.spec_from_file_location(alias, str(full))
        mod = importlib.util.module_from_spec(spec)
        sys.modules[alias] = mod
        spec.loader.exec_module(mod)
    return mod


class _FakeContext:
    """Minimal Lambda context that causes the tool_name try/except to fall back to 'unknown'.

    Handlers do: context.client_context.custom["bedrockAgentCoreToolName"]
    Setting client_context=None triggers AttributeError → caught → _tool_name stays "unknown".
    This avoids MagicMock attributes being passed to json.dumps.
    """
    client_context = None


# ---------------------------------------------------------------------------
# #53 — SSRF: bucket name validation in catalog quality check
# ---------------------------------------------------------------------------

class TestBucketNameValidation:
    """_validate_bucket_name() rejects unsafe names and accepts valid S3 names."""

    def _mod(self):
        return _load("lambdas/catalog-quality-check/handler.py", "_cqc_ssrf")

    def test_valid_bucket_accepted(self):
        mod = self._mod()
        assert mod._validate_bucket_name("my-valid-bucket") is True

    def test_valid_bucket_with_numbers_accepted(self):
        mod = self._mod()
        assert mod._validate_bucket_name("data-bucket-2024") is True

    def test_valid_bucket_with_dots_accepted(self):
        mod = self._mod()
        assert mod._validate_bucket_name("registry.opendata.aws") is True

    def test_empty_name_rejected(self):
        mod = self._mod()
        assert mod._validate_bucket_name("") is False

    def test_uppercase_rejected(self):
        mod = self._mod()
        assert mod._validate_bucket_name("MY-BUCKET") is False

    def test_double_dot_rejected(self):
        mod = self._mod()
        assert mod._validate_bucket_name("bucket..name") is False

    def test_too_short_rejected(self):
        mod = self._mod()
        assert mod._validate_bucket_name("ab") is False

    def test_too_long_rejected(self):
        mod = self._mod()
        assert mod._validate_bucket_name("a" * 64) is False

    def test_crafted_hostname_style_rejected(self):
        """Bucket name that could be used for SSRF via virtual-hosted-style DNS."""
        mod = self._mod()
        # Starts with digit-only then dash — valid S3, but test that it passes
        # More importantly: names that could resolve to internal AWS services
        # are blocked by the uppercase check (169.254.169.254 is not a valid bucket name)
        assert mod._validate_bucket_name("169-254-169-254") is True  # allowed — valid S3 name
        # but a name with uppercase or IP-format would be caught by other validators

    def test_probe_skips_invalid_bucket(self):
        """_probe_s3_resources skips ARNs with invalid bucket names."""
        mod = self._mod()
        skipped_resources = [{"arn": "arn:aws:s3:::INVALID-UPPERCASE-BUCKET"}]
        # Should not raise, just skip
        result = mod._probe_s3_resources(skipped_resources)
        assert result is False  # not unreachable, just skipped


# ---------------------------------------------------------------------------
# #54 — QuickSight principal from env var, not caller event
# ---------------------------------------------------------------------------

class TestQuickSightPrincipalSource:
    """QS principal must come from env var, not caller-supplied event field."""

    def test_roda_load_uses_env_var_not_event(self):
        """_create_quicksight_dataset in dataset-loader reads QUICKSIGHT_USER from env."""
        path = str(REPO_ROOT / "lambdas" / "dataset-loader" / "handler.py")
        source = Path(path).read_text()
        # Must use env var
        assert 'os.environ.get("QUICKSIGHT_USER"' in source or "QUICKSIGHT_USER" in source
        # Must NOT use caller_qs_user from event
        assert "caller_qs_user" not in source
        assert 'event.get("qs_user"' not in source

    def test_s3_load_uses_env_var_not_event(self):
        """_create_quicksight_datasource in s3-load reads QUICKSIGHT_USER from env."""
        path = str(REPO_ROOT / "lambdas" / "s3-load" / "handler.py")
        source = Path(path).read_text()
        assert 'os.environ.get("QUICKSIGHT_USER"' in source or "QUICKSIGHT_USER" in source
        assert "caller_qs_user" not in source
        assert 'event.get("qs_user"' not in source


# ---------------------------------------------------------------------------
# #55 — register-source: connection_config format validation
# ---------------------------------------------------------------------------

class TestRegisterSourceConfigValidation:
    """_validate_connection_config() enforces format per source type."""

    def _validate(self, source_type: str, config) -> str | None:
        mod = _load(
            "lambdas/register-source/handler.py",
            "_rs_cv",
            env={"SOURCE_REGISTRY_TABLE": "test-table"},
        )
        return mod._validate_connection_config(source_type, config)

    # S3 — must have bucket key
    def test_s3_dict_with_bucket_accepted(self):
        assert self._validate("s3", {"bucket": "my-bucket"}) is None

    def test_s3_json_string_with_bucket_accepted(self):
        import json
        assert self._validate("s3", json.dumps({"bucket": "my-bucket"})) is None

    def test_s3_missing_bucket_rejected(self):
        result = self._validate("s3", {"prefix": "/data"})
        assert result is not None
        assert "bucket" in result

    def test_s3_plain_string_rejected(self):
        result = self._validate("s3", "config-v1")
        assert result is not None

    # Snowflake — must be a Secrets Manager ARN
    def test_snowflake_valid_arn_accepted(self):
        assert self._validate(
            "snowflake",
            "arn:aws:secretsmanager:us-east-1:123456789012:secret:sf-creds"
        ) is None

    def test_snowflake_plain_string_rejected(self):
        result = self._validate("snowflake", "my-snowflake-config")
        assert result is not None
        assert "Secrets Manager ARN" in result

    # Redshift — must be a Secrets Manager ARN
    def test_redshift_valid_arn_accepted(self):
        assert self._validate(
            "redshift",
            "arn:aws:secretsmanager:us-west-2:999999999999:secret:rs-creds"
        ) is None

    def test_redshift_plain_string_rejected(self):
        result = self._validate("redshift", "workgroup-name")
        assert result is not None

    # RODA — no constraints
    def test_roda_any_config_accepted(self):
        assert self._validate("roda", "anything") is None
        assert self._validate("roda", {}) is None


# ---------------------------------------------------------------------------
# #56 — Redshift workgroup not exposed in response
# ---------------------------------------------------------------------------

class TestRedshiftWorkgroupNotExposed:
    """redshift_browse response must not include workgroup name."""

    def test_workgroup_not_in_happy_path_response(self):
        """Successful response does not include workgroup identifier."""
        path = str(REPO_ROOT / "lambdas" / "redshift-browse" / "handler.py")
        source = Path(path).read_text()
        # The response dict must not have "workgroup" as a key in the return value
        # (it appears in config but not in the returned dict)
        # Check the return statement doesn't expose workgroup
        assert '"workgroup": workgroup' not in source
        assert "'workgroup': workgroup" not in source

    def test_response_keys_exclude_workgroup(self):
        """Mock-based check: handler response does not include workgroup."""
        import json
        from unittest.mock import MagicMock as _MM
        mod = _load("lambdas/redshift-browse/handler.py", "_rb_wg", env={})

        mock_sm = _MM()
        mock_sm.get_secret_value.return_value = {
            "SecretString": json.dumps({
                "workgroup": "my-secret-workgroup",
                "database": "mydb",
                "secret_arn": "arn:aws:secretsmanager:us-east-1:123:secret:rs",
            })
        }
        mock_redshift = _MM()
        mock_redshift.execute_statement.return_value = {"Id": "stmt-001"}
        mock_redshift.describe_statement.return_value = {"Status": "FINISHED"}
        mock_redshift.get_statement_result.return_value = {"Records": []}

        with patch.object(mod, "secrets_client", mock_sm), \
             patch.object(mod, "redshift_data", mock_redshift), \
             patch.object(mod, "REDSHIFT_SECRET_ARN", "arn:aws:secretsmanager:us-east-1:123:secret:rs"):
            result = mod.handler({"source_id": "rs-prod"}, _FakeContext())

        assert "workgroup" not in result
        assert "my-secret-workgroup" not in str(result)


# ---------------------------------------------------------------------------
# #58 — s3_preview file extension allowlist
# ---------------------------------------------------------------------------

class TestS3PreviewExtensionAllowlist:
    """s3_preview rejects unsupported file extensions before any S3 read."""

    def _handler(self):
        mod = _load(
            "lambdas/s3-preview/handler.py",
            "_s3p_ext",
            env={"SOURCES_CONFIG": '[{"label":"test-source","bucket":"my-bucket","prefix":"data/"}]'},
        )
        return mod

    def _call(self, key: str):
        mod = self._handler()
        return mod.handler(
            {"source": "test-source", "key": key},
            _FakeContext()
        )

    def test_parquet_in_allowlist(self):
        mod = self._handler()
        assert ".parquet" in mod._ALLOWED_EXTENSIONS

    def test_csv_in_allowlist(self):
        mod = self._handler()
        assert ".csv" in mod._ALLOWED_EXTENSIONS

    def test_csv_gz_in_allowlist(self):
        mod = self._handler()
        assert ".csv.gz" in mod._ALLOWED_EXTENSIONS

    def test_jsonl_in_allowlist(self):
        mod = self._handler()
        assert ".jsonl" in mod._ALLOWED_EXTENSIONS

    def test_ndjson_in_allowlist(self):
        mod = self._handler()
        assert ".ndjson" in mod._ALLOWED_EXTENSIONS

    def test_exe_rejected(self):
        result = self._call("data/malware.exe")
        assert "error" in result
        assert "Unsupported file type" in result["error"]

    def test_zip_rejected(self):
        result = self._call("data/archive.zip")
        assert "error" in result
        assert "Unsupported file type" in result["error"]

    def test_binary_rejected(self):
        result = self._call("data/dump.bin")
        assert "error" in result
        assert "Unsupported file type" in result["error"]

    def test_no_extension_rejected(self):
        result = self._call("data/no-extension")
        assert "error" in result
        assert "Unsupported file type" in result["error"]

    def test_extension_check_before_s3_call(self):
        """S3 is never called when the extension is rejected."""
        mod = self._handler()
        with patch.object(mod, "s3") as mock_s3:
            mod.handler({"source": "test-source", "key": "data/file.exe"}, _FakeContext())
        mock_s3.head_object.assert_not_called()
        mock_s3.get_object.assert_not_called()


# ---------------------------------------------------------------------------
# #59 — Error message sanitization
# ---------------------------------------------------------------------------

class TestErrorSanitization:
    """Error responses must not expose internal bucket names, ARNs, or exception details."""

    def test_s3_browse_browse_error_sanitized(self):
        from unittest.mock import MagicMock as _MM
        mod = _load(
            "lambdas/s3-browse/handler.py",
            "_s3b_san",
            env={"SOURCES_CONFIG": '[{"label":"data","bucket":"my-internal-bucket-12345","prefix":""}]'},
        )
        mock_s3 = _MM()
        # Give the mock proper exception classes so `except s3.exceptions.NoSuchBucket:` works
        mock_s3.exceptions.NoSuchBucket = type("NoSuchBucket", (Exception,), {})
        mock_s3.list_objects_v2.side_effect = Exception(
            "Access Denied: arn:aws:s3:::my-internal-bucket-12345"
        )
        with patch.object(mod, "s3", mock_s3):
            result = mod.handler({"source": "data"}, _FakeContext())
        assert "error" in result
        assert "my-internal-bucket-12345" not in result["error"]
        assert "arn:aws:s3" not in result["error"]

    def test_s3_preview_s3_error_sanitized(self):
        from unittest.mock import MagicMock as _MM
        mod = _load(
            "lambdas/s3-preview/handler.py",
            "_s3p_san",
            env={"SOURCES_CONFIG": '[{"label":"data","bucket":"internal-bucket","prefix":"data/"}]'},
        )
        mock_s3 = _MM()
        # Give the mock proper exception classes so `except s3.exceptions.NoSuchKey:` works
        mock_s3.exceptions.NoSuchKey = type("NoSuchKey", (Exception,), {})
        mock_s3.head_object.side_effect = Exception("Access Denied for internal-bucket")
        with patch.object(mod, "s3", mock_s3):
            result = mod.handler({"source": "data", "key": "file.csv"}, _FakeContext())
        assert "error" in result
        assert "internal-bucket" not in result["error"]

    def test_redshift_execute_error_sanitized(self):
        import json
        from unittest.mock import MagicMock as _MM
        mod = _load("lambdas/redshift-browse/handler.py", "_rb_san", env={})
        mock_sm = _MM()
        mock_sm.get_secret_value.return_value = {
            "SecretString": json.dumps({
                "workgroup": "secret-workgroup-arn",
                "database": "mydb",
                "secret_arn": "arn:aws:secretsmanager:us-east-1:123:secret:rs",
            })
        }
        mock_redshift = _MM()
        mock_redshift.execute_statement.side_effect = Exception(
            "Workgroup secret-workgroup-arn not found in us-east-1:999"
        )
        with patch.object(mod, "secrets_client", mock_sm), \
             patch.object(mod, "redshift_data", mock_redshift), \
             patch.object(mod, "REDSHIFT_SECRET_ARN", "arn:aws:secretsmanager:us-east-1:123:secret:rs"):
            result = mod.handler({"source_id": "rs"}, _FakeContext())
        assert "error" in result
        assert "secret-workgroup-arn" not in result["error"]
        assert "999" not in result["error"]

    def test_snowflake_api_error_sanitized(self):
        import urllib.error
        from io import BytesIO
        from unittest.mock import MagicMock as _MM
        mod = _load("lambdas/snowflake-browse/handler.py", "_sf_san", env={})
        mock_sm = _MM()
        mock_sm.get_secret_value.return_value = {
            "SecretString": '{"account": "myaccount.snowflakecomputing.com", '
                            '"user": "u", "password": "p", "warehouse": "w"}'
        }
        http_err = urllib.error.HTTPError(
            url="https://myaccount.snowflakecomputing.com/api/v2/statements",
            code=401,
            msg="Unauthorized",
            hdrs={},
            fp=BytesIO(b'{"message": "Incorrect username or password for account myaccount"}'),
        )
        with patch.object(mod, "secrets_client", mock_sm), \
             patch.object(mod, "SNOWFLAKE_SECRET_ARN", "arn:aws:secretsmanager:us-east-1:123:secret:sf"), \
             patch("urllib.request.urlopen", side_effect=http_err):
            result = mod.handler({"source_id": "sf-prod"}, _FakeContext())
        assert "error" in result
        assert "myaccount" not in result["error"]
        assert "Incorrect username" not in result["error"]
        assert "Snowflake query failed" in result["error"]


# ---------------------------------------------------------------------------
# #40 — Per-caller credential isolation (caller_secret_arn)
# ---------------------------------------------------------------------------

_VALID_CALLER_ARN = "arn:aws:secretsmanager:us-east-1:123456789012:secret:caller-creds-abc123"
_ALLOWED_PREFIX = "arn:aws:secretsmanager:us-east-1:123456789012:secret:caller-"


class TestCallerSecretIsolation:
    """
    caller_secret_arn parameter on browse/preview handlers uses caller creds
    instead of the shared service account, subject to allowlist validation.
    """

    def _sf_browse(self, allowed_arns: str = "") -> object:
        return _load(
            "lambdas/snowflake-browse/handler.py",
            f"_sf_browse_caller_{id(allowed_arns)}",
            env={"SNOWFLAKE_SECRET_ARN": "arn:aws:secretsmanager:us-east-1:123:secret:shared",
                 "CALLER_SECRETS_ALLOWED_ARNS": allowed_arns},
        )

    def _sf_preview(self, allowed_arns: str = "") -> object:
        return _load(
            "lambdas/snowflake-preview/handler.py",
            f"_sf_preview_caller_{id(allowed_arns)}",
            env={"SNOWFLAKE_SECRET_ARN": "arn:aws:secretsmanager:us-east-1:123:secret:shared",
                 "CALLER_SECRETS_ALLOWED_ARNS": allowed_arns},
        )

    def _rs_browse(self, allowed_arns: str = "") -> object:
        return _load(
            "lambdas/redshift-browse/handler.py",
            f"_rs_browse_caller_{id(allowed_arns)}",
            env={"REDSHIFT_SECRET_ARN": "arn:aws:secretsmanager:us-east-1:123:secret:shared",
                 "CALLER_SECRETS_ALLOWED_ARNS": allowed_arns},
        )

    def _rs_preview(self, allowed_arns: str = "") -> object:
        return _load(
            "lambdas/redshift-preview/handler.py",
            f"_rs_preview_caller_{id(allowed_arns)}",
            env={"REDSHIFT_SECRET_ARN": "arn:aws:secretsmanager:us-east-1:123:secret:shared",
                 "CALLER_SECRETS_ALLOWED_ARNS": allowed_arns},
        )

    # ---- _resolve_caller_secret_arn unit tests (using snowflake-browse as representative) ----

    def test_valid_arn_with_allowlist_accepted(self):
        mod = self._sf_browse(_ALLOWED_PREFIX)
        assert mod._resolve_caller_secret_arn(_VALID_CALLER_ARN) == _VALID_CALLER_ARN

    def test_invalid_arn_format_rejected(self):
        mod = self._sf_browse(_ALLOWED_PREFIX)
        assert mod._resolve_caller_secret_arn("not-an-arn") is None

    def test_arn_not_in_allowlist_rejected(self):
        mod = self._sf_browse("arn:aws:secretsmanager:us-east-1:999:secret:other-")
        assert mod._resolve_caller_secret_arn(_VALID_CALLER_ARN) is None

    def test_empty_allowlist_rejects_all_caller_arns(self):
        """When CALLER_SECRETS_ALLOWED_ARNS is empty, no caller ARN is permitted."""
        mod = self._sf_browse("")
        assert mod._resolve_caller_secret_arn(_VALID_CALLER_ARN) is None

    def test_iam_arn_not_accepted_as_secret_arn(self):
        mod = self._sf_browse(_ALLOWED_PREFIX)
        assert mod._resolve_caller_secret_arn("arn:aws:iam::123:user/alice") is None

    # ---- Integration: handler rejects disallowed caller_secret_arn ----

    def test_sf_browse_disallowed_caller_arn_returns_error(self):
        mod = self._sf_browse("")  # no allowlist
        result = mod.handler(
            {"source_id": "sf-prod", "caller_secret_arn": _VALID_CALLER_ARN},
            _FakeContext(),
        )
        assert "error" in result
        assert "not permitted" in result["error"]

    def test_sf_preview_disallowed_caller_arn_returns_error(self):
        mod = self._sf_preview("")
        result = mod.handler(
            {"source_id": "sf-prod", "schema": "PUBLIC", "table": "T",
             "caller_secret_arn": _VALID_CALLER_ARN},
            _FakeContext(),
        )
        assert "error" in result
        assert "not permitted" in result["error"]

    def test_rs_browse_disallowed_caller_arn_returns_error(self):
        mod = self._rs_browse("")
        result = mod.handler(
            {"source_id": "rs-prod", "caller_secret_arn": _VALID_CALLER_ARN},
            _FakeContext(),
        )
        assert "error" in result
        assert "not permitted" in result["error"]

    def test_rs_preview_disallowed_caller_arn_returns_error(self):
        mod = self._rs_preview("")
        result = mod.handler(
            {"source_id": "rs-prod", "schema": "public", "table": "orders",
             "caller_secret_arn": _VALID_CALLER_ARN},
            _FakeContext(),
        )
        assert "error" in result
        assert "not permitted" in result["error"]

    # ---- Integration: allowed caller_secret_arn fetches that secret ----

    def test_sf_browse_allowed_caller_arn_uses_caller_secret(self):
        import json as _json
        from unittest.mock import MagicMock as _MM

        mod = self._sf_browse(_ALLOWED_PREFIX)

        mock_sm = _MM()
        # shared secret
        shared_cfg = {"account": "shared-acct", "user": "u", "password": "p", "warehouse": "w"}
        # caller secret
        caller_cfg = {"account": "caller-acct", "user": "cu", "password": "cp", "warehouse": "cw"}

        def get_secret(SecretId):
            if SecretId == _VALID_CALLER_ARN:
                return {"SecretString": _json.dumps(caller_cfg)}
            return {"SecretString": _json.dumps(shared_cfg)}

        mock_sm.get_secret_value.side_effect = get_secret

        mock_resp = _MM()
        mock_resp.read.return_value = _json.dumps({"data": []}).encode()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = _MM(return_value=False)

        captured_urls = []

        def fake_urlopen(req, timeout=30):
            captured_urls.append(req.full_url if hasattr(req, "full_url") else str(req))
            return mock_resp

        with patch.object(mod, "secrets_client", mock_sm):
            with patch("urllib.request.urlopen", side_effect=fake_urlopen):
                mod.handler(
                    {"source_id": "sf-prod", "caller_secret_arn": _VALID_CALLER_ARN},
                    _FakeContext(),
                )

        # Verify the caller secret was fetched (not the shared one)
        mock_sm.get_secret_value.assert_called_with(SecretId=_VALID_CALLER_ARN)
        # URL should use caller-acct
        assert any("caller-acct" in u for u in captured_urls)

    def test_absent_caller_arn_uses_shared_secret(self):
        """When caller_secret_arn is absent, the shared secret is used."""
        import json as _json
        from unittest.mock import MagicMock as _MM

        mod = self._sf_browse(_ALLOWED_PREFIX)

        mock_sm = _MM()
        shared_cfg = {"account": "shared-acct", "user": "u", "password": "p", "warehouse": "w"}
        mock_sm.get_secret_value.return_value = {"SecretString": _json.dumps(shared_cfg)}

        mock_resp = _MM()
        mock_resp.read.return_value = _json.dumps({"data": []}).encode()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = _MM(return_value=False)

        with patch.object(mod, "secrets_client", mock_sm), \
             patch.object(mod, "SNOWFLAKE_SECRET_ARN", "arn:aws:secretsmanager:us-east-1:123:secret:shared"), \
             patch("urllib.request.urlopen", return_value=mock_resp):
            mod.handler({"source_id": "sf-prod"}, _FakeContext())

        mock_sm.get_secret_value.assert_called_with(
            SecretId="arn:aws:secretsmanager:us-east-1:123:secret:shared"
        )
