"""
redshift_query: Execute a parameterized read-only SQL query against Redshift Serverless.

AgentCore Lambda target — invoked directly by the Gateway.
Event dict contains tool arguments. Returns a plain dict.

Uses boto3 redshift-data client (Redshift Data API async pattern).

Tool arguments:
  connection_id: str (required) — source ID from qs-data-source-registry
  query:         str (required) — parameterized SQL with ? placeholders (Redshift uses $1, $2 internally but we substitute ? for consistency)
  params:        list (optional) — bind parameter values (strings/numbers)
  max_rows:      int (optional, default 100, max 1000)
"""

import json
import logging
import os
import re
import time
from typing import Any

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

secrets_client = boto3.client("secretsmanager")
redshift_data = boto3.client("redshift-data")

REDSHIFT_SECRET_ARN = os.environ.get("REDSHIFT_SECRET_ARN", "")
CALLER_SECRETS_ALLOWED_ARNS: list[str] = [
    p.strip()
    for p in os.environ.get("CALLER_SECRETS_ALLOWED_ARNS", "").split(",")
    if p.strip()
]
_SECRET_ARN_RE = re.compile(r"^arn:aws:secretsmanager:[a-z0-9\-]+:\d{12}:secret:.+$")
_MAX_ROWS = 1000

_POLL_MAX = 60
_POLL_INTERVAL = 1

# Mutation detection
_MUTATION_PATTERN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|TRUNCATE|ALTER|MERGE|REPLACE|UPSERT)\b",
    re.IGNORECASE,
)


def _resolve_caller_secret_arn(caller_arn: str) -> str | None:
    """Validate a caller-supplied secret ARN against the allowlist."""
    if not _SECRET_ARN_RE.match(caller_arn):
        return None
    if not CALLER_SECRETS_ALLOWED_ARNS:
        return None
    if any(caller_arn.startswith(prefix) for prefix in CALLER_SECRETS_ALLOWED_ARNS):
        return caller_arn
    return None


def _get_redshift_config(secret_arn: str | None = None) -> dict | None:
    """Fetch Redshift connection config from Secrets Manager."""
    arn = secret_arn or REDSHIFT_SECRET_ARN
    if not arn:
        return None
    try:
        resp = secrets_client.get_secret_value(SecretId=arn)
        return json.loads(resp["SecretString"])
    except Exception as e:
        logger.error(json.dumps({"error": "secrets_manager_error", "detail": str(e)}))
        return None


def _poll_statement(statement_id: str) -> dict:
    """Poll DescribeStatement until terminal status or timeout."""
    for _ in range(_POLL_MAX):
        resp = redshift_data.describe_statement(Id=statement_id)
        status = resp.get("Status", "")
        if status in ("FINISHED", "FAILED", "ABORTED"):
            return resp
        time.sleep(_POLL_INTERVAL)
    raise TimeoutError(f"Redshift statement {statement_id} did not complete in {_POLL_MAX}s")


def _replace_placeholders(query: str, params: list) -> tuple[str, list]:
    """
    Replace ? placeholders with $1, $2, ... for Redshift Data API.

    Returns (rewritten_query, params_for_api).
    The Redshift Data API uses positional parameters as SQL_PARAMETERS list.
    """
    idx = 0
    result = []
    for ch in query:
        if ch == "?":
            idx += 1
            result.append(f"${idx}")
        else:
            result.append(ch)
    return "".join(result), params


def handler(event: dict, context: Any) -> dict:
    _tool_name = "unknown"
    try:
        raw = context.client_context.custom["bedrockAgentCoreToolName"]
        _tool_name = raw.split("___")[-1]
    except Exception:
        pass
    logger.info(json.dumps({"tool": _tool_name, "connection_id": event.get("connection_id")}))

    connection_id = (event.get("connection_id") or "").strip()
    if not connection_id:
        return {"error": "connection_id is required"}

    query = (event.get("query") or "").strip()
    if not query:
        return {"error": "query is required"}

    if _MUTATION_PATTERN.search(query):
        return {"error": "Only read-only SELECT queries are permitted"}

    params = event.get("params") or []
    if not isinstance(params, list):
        return {"error": "params must be a list"}

    try:
        max_rows = min(int(event.get("max_rows", 100)), _MAX_ROWS)
    except (TypeError, ValueError):
        max_rows = 100

    caller_arn = (event.get("caller_secret_arn") or "").strip()
    resolved_arn: str | None = None
    if caller_arn:
        resolved_arn = _resolve_caller_secret_arn(caller_arn)
        if resolved_arn is None:
            return {"error": "caller_secret_arn is not permitted"}

    config = _get_redshift_config(resolved_arn)
    if config is None:
        return {"error": "Redshift source not configured"}

    workgroup = config.get("workgroup", "")
    database = config.get("database", "")
    secret_arn = config.get("secret_arn", "")

    # Rewrite ? → $1, $2, ... for Redshift
    rewritten_query, _ = _replace_placeholders(query, params)

    # Append LIMIT if absent
    if not re.search(r"\bLIMIT\b", rewritten_query, re.IGNORECASE):
        rewritten_query = f"{rewritten_query} LIMIT {max_rows}"

    # Build SQL parameters for Redshift Data API
    sql_parameters = [{"name": str(i + 1), "value": str(v)} for i, v in enumerate(params)]

    exec_kwargs: dict[str, Any] = {
        "WorkgroupName": workgroup,
        "Database": database,
        "SecretArn": secret_arn,
        "Sql": rewritten_query,
    }
    if sql_parameters:
        exec_kwargs["Parameters"] = sql_parameters

    try:
        exec_resp = redshift_data.execute_statement(**exec_kwargs)
        statement_id = exec_resp["Id"]
    except Exception as e:
        logger.error(json.dumps({"redshift_error": str(e)}))
        return {"error": "Redshift query could not be submitted"}

    try:
        final = _poll_statement(statement_id)
    except TimeoutError:
        return {"error": "Redshift query timed out"}
    except Exception as e:
        logger.error(json.dumps({"poll_error": str(e)}))
        return {"error": "Redshift query status check failed"}

    status = final.get("Status", "")
    if status in ("FAILED", "ABORTED"):
        logger.error(json.dumps({"redshift_query_status": status, "detail": final.get("Error", "")}))
        return {"error": "Redshift query failed"}

    try:
        result = redshift_data.get_statement_result(Id=statement_id)
    except Exception as e:
        logger.error(json.dumps({"get_result_error": str(e)}))
        return {"error": "Failed to retrieve Redshift query results"}

    column_metadata = result.get("ColumnMetadata", [])
    columns = [col.get("name", f"col{i}") for i, col in enumerate(column_metadata)]

    records = result.get("Records", [])
    if not columns and records:
        columns = [f"col{i}" for i in range(len(records[0]))]

    rows = []
    for row in records[:max_rows]:
        row_dict = {}
        for i, cell in enumerate(row):
            col_name = columns[i] if i < len(columns) else f"col{i}"
            val = None
            for cell_val in cell.values():
                val = cell_val
                break
            row_dict[col_name] = val
        rows.append(row_dict)

    return {
        "connection_id": connection_id,
        "columns": columns,
        "rows": rows,
        "row_count": len(rows),
    }
