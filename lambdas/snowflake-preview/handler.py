"""
snowflake_preview: Sample rows from a Snowflake table.

AgentCore Lambda target — invoked directly by the Gateway.
Event dict contains tool arguments. Returns a plain dict.

Uses urllib.request and base64 from stdlib only. No snowflake-connector-python.
"""

import base64
import json
import logging
import os
import re
import urllib.error
import urllib.request
from typing import Any

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

secrets_client = boto3.client("secretsmanager")


def _compute_quality(rows: list[dict]) -> dict:
    """Compute data quality metrics from sample rows (#38)."""
    if not rows:
        return {"row_count": 0, "null_pct": {}, "estimated_cardinality": {}, "duplicate_row_pct": 0.0}

    row_count = len(rows)
    columns = list(rows[0].keys()) if rows else []

    null_pct = {}
    cardinality = {}
    for col in columns:
        values = [r.get(col) for r in rows]
        null_count = sum(1 for v in values if v is None or v == "")
        null_pct[col] = round(null_count / row_count * 100, 1)
        unique_values = len({str(v) for v in values if v is not None and v != ""})
        cardinality[col] = unique_values

    # Duplicate detection
    row_strings = [json.dumps(r, sort_keys=True, default=str) for r in rows]
    dup_count = row_count - len(set(row_strings))

    return {
        "row_count": row_count,
        "null_pct": null_pct,
        "estimated_cardinality": cardinality,
        "duplicate_row_pct": round(dup_count / row_count * 100, 1) if row_count > 0 else 0.0,
    }

SNOWFLAKE_SECRET_ARN = os.environ.get("SNOWFLAKE_SECRET_ARN", "")
CALLER_SECRETS_ALLOWED_ARNS: list[str] = [
    p.strip()
    for p in os.environ.get("CALLER_SECRETS_ALLOWED_ARNS", "").split(",")
    if p.strip()
]
_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z0-9_]+$")
_SECRET_ARN_RE = re.compile(r"^arn:aws:secretsmanager:[a-z0-9\-]+:\d{12}:secret:.+$")


def _resolve_caller_secret_arn(caller_arn: str) -> str | None:
    """Validate a caller-supplied secret ARN against the allowlist."""
    if not _SECRET_ARN_RE.match(caller_arn):
        return None
    if not CALLER_SECRETS_ALLOWED_ARNS:
        return None
    if any(caller_arn.startswith(prefix) for prefix in CALLER_SECRETS_ALLOWED_ARNS):
        return caller_arn
    return None


def _get_snowflake_config(secret_arn: str | None = None) -> dict | None:
    """Fetch Snowflake connection config from Secrets Manager. Returns None if not configured."""
    arn = secret_arn or SNOWFLAKE_SECRET_ARN
    if not arn:
        return None
    try:
        resp = secrets_client.get_secret_value(SecretId=arn)
        return json.loads(resp["SecretString"])
    except Exception as e:
        logger.error(json.dumps({"error": "secrets_manager_error", "detail": str(e)}))
        return None


def _snowflake_execute(config: dict, statement: str) -> dict:
    """
    Execute a SQL statement via the Snowflake SQL API v2.

    Returns the parsed JSON response body or raises an exception.
    """
    account = config["account"]
    user = config["user"]
    password = config["password"]
    warehouse = config.get("warehouse", "")
    role = config.get("role", "")
    database = config.get("database", "")

    url = f"https://{account}.snowflakecomputing.com/api/v2/statements"
    credentials = base64.b64encode(f"{user}:{password}".encode()).decode()

    body = {
        "statement": statement,
        "warehouse": warehouse,
        "role": role,
        "database": database,
    }
    body_bytes = json.dumps(body).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=body_bytes,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Basic {credentials}",
        },
    )

    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def handler(event: dict, context: Any) -> dict:
    """
    Sample rows from a Snowflake table.

    Tool arguments:
    - source_id: str (required) — identifier for the Snowflake source
    - schema: str (required) — table schema
    - table: str (required) — table name
    - max_rows: int (optional, default 5, max 25)
    """
    _tool_name = "unknown"
    try:
        raw = context.client_context.custom["bedrockAgentCoreToolName"]
        _tool_name = raw.split("___")[-1]
    except Exception:
        pass
    logger.info(json.dumps({"tool": _tool_name, "event": event}))

    source_id = (event.get("source_id") or "").strip()
    if not source_id:
        return {"error": "source_id is required"}

    schema = (event.get("schema") or "").strip()
    if not schema:
        return {"error": "schema is required"}

    table = (event.get("table") or "").strip()
    if not table:
        return {"error": "table is required"}

    # Sanitize schema and table names — alphanumeric + underscore only
    if not _SAFE_IDENTIFIER.match(schema):
        return {"error": "invalid table name"}
    if not _SAFE_IDENTIFIER.match(table):
        return {"error": "invalid table name"}

    try:
        max_rows = int(event.get("max_rows", 5))
    except (TypeError, ValueError):
        max_rows = 5
    max_rows = min(max(1, max_rows), 25)

    caller_arn = (event.get("caller_secret_arn") or "").strip()
    resolved_arn: str | None = None
    if caller_arn:
        resolved_arn = _resolve_caller_secret_arn(caller_arn)
        if resolved_arn is None:
            return {"error": "caller_secret_arn is not permitted"}

    config = _get_snowflake_config(resolved_arn)
    if config is None:
        return {"error": "Snowflake source not configured"}

    sql = f'SELECT * FROM "{schema}"."{table}" LIMIT {max_rows}'

    try:
        result = _snowflake_execute(config, sql)
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8")
        except Exception:
            pass
        logger.error(json.dumps({"snowflake_http_error": e.code, "body": body}))
        return {"error": f"Snowflake API error: {body or str(e)}"}
    except Exception as e:
        logger.error(json.dumps({"snowflake_error": str(e)}))
        return {"error": f"Snowflake API error: {e}"}

    # Parse result — columns from resultSetMetaData, rows from data
    metadata = result.get("resultSetMetaData", {})
    row_type = metadata.get("rowType", [])
    columns = [col.get("name", f"col{i}") for i, col in enumerate(row_type)]

    rows = result.get("data", [])

    # Infer columns from first row if metadata not available
    if not columns and rows:
        columns = [f"col{i}" for i in range(len(rows[0]))]

    sample_rows = []
    for row in rows:
        sample_rows.append(dict(zip(columns, row)))

    return {
        "source_id": source_id,
        "schema": schema,
        "table": table,
        "columns": columns,
        "sample_rows": sample_rows,
        "row_count": len(sample_rows),
        "format": "snowflake",
        "quality": _compute_quality(sample_rows),
    }
