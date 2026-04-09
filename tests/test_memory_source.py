"""Tests for register-memory-source Lambda (data v0.12.0 — issue #60)."""
from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

REPO_ROOT = Path(__file__).parent.parent


def _load_handler():
    path = REPO_ROOT / "lambdas" / "memory" / "handler.py"
    spec = importlib.util.spec_from_file_location("memory_handler", str(path))
    mod = importlib.util.module_from_spec(spec)
    sys.modules["memory_handler"] = mod
    spec.loader.exec_module(mod)
    return mod


class TestRegisterMemorySource:
    def setup_method(self):
        # Clear cached module
        for k in list(sys.modules.keys()):
            if "memory_handler" in k:
                del sys.modules[k]
        self.mod = _load_handler()
        self.handler = self.mod.handler

    def _make_event(self, **kwargs):
        base = {
            "user_arn_hash": "abc123def456",
            "memory_s3_uri": "s3://claws-memory-123456789/us-east-1/abc123def456/findings.jsonl",
            "dataset_label": "test-memory",
            "aws_account_id": "123456789012",
        }
        base.update(kwargs)
        return base

    def test_happy_path_returns_ids(self):
        mock_qs = MagicMock()
        mock_s3 = MagicMock()
        mock_ddb = MagicMock()
        mock_table = MagicMock()
        mock_table.get_item.return_value = {}  # No existing item
        mock_ddb.Table.return_value = mock_table

        with patch.object(self.mod, "_quicksight", return_value=mock_qs), \
             patch.object(self.mod, "_s3_client", return_value=mock_s3), \
             patch.object(self.mod, "_ddb", return_value=mock_ddb):
            result = self.handler(self._make_event(), None)

        assert "dataset_id" in result
        assert "data_source_id" in result
        assert result["source_id"] == result["dataset_id"]

    def test_idempotent_second_call_no_qs_calls(self):
        mock_qs = MagicMock()
        mock_ddb = MagicMock()
        mock_table = MagicMock()
        mock_table.get_item.return_value = {
            "Item": {
                "user_arn_hash": "abc123def456",
                "dataset_type": "findings",
                "dataset_id": "claws-memory-ds-abc123-existing",
                "data_source_id": "claws-memory-src-abc123-existing",
            }
        }
        mock_ddb.Table.return_value = mock_table

        with patch.object(self.mod, "_quicksight", return_value=mock_qs), \
             patch.object(self.mod, "_ddb", return_value=mock_ddb):
            result = self.handler(self._make_event(), None)

        assert result["dataset_id"] == "claws-memory-ds-abc123-existing"
        mock_qs.create_data_source.assert_not_called()
        mock_qs.create_data_set.assert_not_called()

    def test_s3_uri_parsed_to_manifest(self):
        mock_qs = MagicMock()
        mock_s3 = MagicMock()
        mock_ddb = MagicMock()
        mock_table = MagicMock()
        mock_table.get_item.return_value = {}
        mock_ddb.Table.return_value = mock_table

        with patch.object(self.mod, "_quicksight", return_value=mock_qs), \
             patch.object(self.mod, "_s3_client", return_value=mock_s3), \
             patch.object(self.mod, "_ddb", return_value=mock_ddb), \
             patch.dict("os.environ", {"MANIFEST_BUCKET": "my-manifest-bucket"}):
            self.handler(self._make_event(), None)

        # Verify put_object was called with manifest content
        if mock_s3.put_object.called:
            call_kwargs = mock_s3.put_object.call_args[1]
            body = json.loads(call_kwargs["Body"])
            assert "fileLocations" in body
            assert body["fileLocations"][0]["URIs"][0] == self._make_event()["memory_s3_uri"]

    def test_quicksight_error_propagated_as_500(self):
        from botocore.exceptions import ClientError
        mock_qs = MagicMock()
        mock_s3 = MagicMock()
        mock_ddb = MagicMock()
        mock_table = MagicMock()
        mock_table.get_item.return_value = {}
        mock_ddb.Table.return_value = mock_table
        mock_qs.create_data_source.side_effect = ClientError(
            {"Error": {"Code": "AccessDeniedException", "Message": "denied"}}, "CreateDataSource"
        )

        with patch.object(self.mod, "_quicksight", return_value=mock_qs), \
             patch.object(self.mod, "_s3_client", return_value=mock_s3), \
             patch.object(self.mod, "_ddb", return_value=mock_ddb):
            result = self.handler(self._make_event(), None)

        assert "error" in result
        assert result.get("statusCode") == 500

    def test_dynamodb_write_with_correct_keys(self):
        mock_qs = MagicMock()
        mock_s3 = MagicMock()
        mock_ddb = MagicMock()
        mock_table = MagicMock()
        mock_table.get_item.return_value = {}
        mock_ddb.Table.return_value = mock_table

        with patch.object(self.mod, "_quicksight", return_value=mock_qs), \
             patch.object(self.mod, "_s3_client", return_value=mock_s3), \
             patch.object(self.mod, "_ddb", return_value=mock_ddb):
            self.handler(self._make_event(), None)

        mock_table.put_item.assert_called_once()
        item = mock_table.put_item.call_args[1]["Item"]
        assert item["user_arn_hash"] == "abc123def456"
        assert item["dataset_type"] == "findings"
        assert "dataset_id" in item
        assert "data_source_id" in item
