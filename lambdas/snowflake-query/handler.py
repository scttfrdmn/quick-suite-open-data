"""
snowflake_query: Execute a parameterized read-only SQL query against Snowflake.

AgentCore Lambda target — invoked directly by the Gateway.
Event dict contains tool arguments. Returns a plain dict.

Uses urllib.request and base64 from stdlib only. No snowflake-connector-python.

Tool arguments:
  connection_id: str (required) — source ID from qs-data-source-registry
  query:         str (required) — parameterized SQL with ? placeholders
  params:        list (optional) — bind parameter values (strings/numbers)
  max_rows:      int (optional, default 100, max 1000)
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

SNOWFLAKE_SECRET_ARN = os.environ.get("SNOWFLAKE_SECRET_ARN", "")
CALLER_SECRETS_ALLOWED_ARNS: list[str] = [
    p.strip()
    for p in os.environ.get("CALLER_SECRETS_ALLOWED_ARNS", "").split(",")
    if p.strip()
]
_SECRET_ARN_RE = re.compile(r"^arn:aws:secretsmanager:[a-z0-9\-]+:\d{12}:secret:.+$")
_MAX_ROWS = 1000

# Mutation detection — reject any query that modifies data
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


def _get_snowflake_config(secret_arn: str | None = None) -> dict | None:
    """Fetch Snowflake connection config from Secrets Manager."""
    arn = secret_arn or SNOWFLAKE_SECRET_ARN
    if not arn:
        return None
    try:
        resp = secrets_client.get_secret_value(SecretId=arn)
        return json.loads(resp["SecretString"])
    except Exception as e:
        logger.error(json.dumps({"error": "secrets_manager_error", "detail": str(e)}))
        return None


def _snowflake_execute(config: dict, statement: str, bindings: dict | None = None) -> dict:
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

    body: dict[str, Any] = {
        "statement": statement,
        "warehouse": warehouse,
        "role": role,
        "database": database,
    }
    if bindings:
        body["bindings"] = bindings

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

    config = _get_snowflake_config(resolved_arn)
    if config is None:
        return {"error": "Snowflake source not configured"}

    # Build Snowflake SQL API v2 bindings: {"1": {"type": "TEXT", "value": "..."}, ...}
    bindings: dict[str, dict] | None = None
    if params:
        bindings = {}
        for i, val in enumerate(params):
            if isinstance(val, bool):
                bindings[str(i + 1)] = {"type": "BOOLEAN", "value": str(val).lower()}
            elif isinstance(val, int):
                bindings[str(i + 1)] = {"type": "FIXED", "value": str(val)}
            elif isinstance(val, float):
                bindings[str(i + 1)] = {"type": "REAL", "value": str(val)}
            else:
                bindings[str(i + 1)] = {"type": "TEXT", "value": str(val)}

    # Append LIMIT clause if not already present
    if not re.search(r"\bLIMIT\b", query, re.IGNORECASE):
        query = f"{query} LIMIT {max_rows}"

    try:
        result = _snowflake_execute(config, query, bindings)
    except urllib.error.HTTPError as e:
        logger.error(json.dumps({"snowflake_http_error": e.code}))
        return {"error": "Snowflake query failed"}
    except Exception as e:
        logger.error(json.dumps({"snowflake_error": str(e)}))
        return {"error": "Snowflake query failed"}

    metadata = result.get("resultSetMetaData", {})
    row_type = metadata.get("rowType", [])
    columns = [col.get("name", f"col{i}") for i, col in enumerate(row_type)]

    rows = result.get("data", [])
    if not columns and rows:
        columns = [f"col{i}" for i in range(len(rows[0]))]

    sample_rows = [dict(zip(columns, row)) for row in rows[:max_rows]]

    return {
        "connection_id": connection_id,
        "columns": columns,
        "rows": sample_rows,
        "row_count": len(sample_rows),
    }
