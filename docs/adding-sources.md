# Adding a New Data Source

This guide explains how to add a new AgentCore Lambda target data source to
`quick-suite-data`. Follow the patterns below to keep the codebase consistent.

---

## Source Interface Contract

Every data source is an **AgentCore Gateway Lambda target**. The Gateway invokes
your Lambda directly — there is no API Gateway envelope.

**Handler signature:**

```python
def handler(event: dict, context: Any) -> dict:
    ...
    return {...}  # plain dict, no statusCode/body wrapping
```

**Required return fields** for tool responses:

| Field | Type | Description |
|-------|------|-------------|
| `source_type` | str | Stable identifier for the source (e.g. `"pubmed"`) |
| `query` | str | The original query echoed back |
| `results` | list | List of result dicts |
| `count` | int | `len(results)` |

Each result dict must include:

| Field | Type | Description |
|-------|------|-------------|
| `source_id` | str | `"{type}/{identifier}"` format (e.g. `"pubmed/38000001"`) |
| `source_type` | str | Same as top-level `source_type` |

**Error responses** (missing required args):

```python
return {"error": "query is required"}
```

Do not raise exceptions for expected error conditions (missing args, API errors).
Return `{"error": "..."}` for invalid input and empty `results` lists for API
failures (after logging a warning).

---

## Auth Patterns

### Open (no auth)

Public APIs need no credentials. Pass a `User-Agent` header for good citizenship:

```python
req = urllib.request.Request(
    url,
    headers={"Accept": "application/json", "User-Agent": "quick-suite-data/1.0"},
)
with urllib.request.urlopen(req, timeout=20) as resp:
    data = json.loads(resp.read().decode("utf-8"))
```

### API Key (env var)

Read from an environment variable set in the CDK construct. Fail gracefully
(or return an informational note) when the key is absent:

```python
MY_API_KEY = os.environ.get("MY_API_KEY", "")

if not MY_API_KEY:
    return {"source_type": "mysource", "query": query, "results": [], "count": 0,
            "note": "Configure MY_API_KEY env var"}

headers["Authorization"] = f"Token {MY_API_KEY}"
```

### OAuth / Secrets Manager

For credentials stored in Secrets Manager, retrieve at handler invocation time
(not at module level — Lambdas can be reused across requests):

```python
import boto3, json
secrets = boto3.client("secretsmanager")

def _get_secret(arn: str) -> dict:
    resp = secrets.get_secret_value(SecretId=arn)
    return json.loads(resp["SecretString"])
```

Grant `secretsmanager:GetSecretValue` on the specific ARN in the CDK construct.

---

## Quality Score Guidelines

Return a `quality_score` float in `[0.0, 1.0]` on every result to help
federated ranking. If you cannot compute a meaningful score, return `None`.

**Freshness-based** (for preprints, recent publications):

```python
def _recency_score(year: int | None) -> float:
    if year is None:
        return 0.3
    current = datetime.utcnow().year
    if year >= current:    return 1.0
    if year >= current-1:  return 0.8
    if year >= current-2:  return 0.6
    return 0.3
```

**Citation-based blend** (for indexed literature):

```python
def _quality_score(citation_count: int, year: int | None) -> float:
    citation_component = min(1.0, citation_count / 100) * 0.6
    recency_component  = _recency_score(year) * 0.4
    return round(citation_component + recency_component, 3)
```

---

## Testing Skeleton

Use `_make_urlopen_response` to mock HTTP responses and `patch` to intercept
`urllib.request.urlopen`. Do not use `requests` or other HTTP libraries.

```python
import importlib.util, os, sys, json
from unittest.mock import MagicMock, patch

REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")

def _make_urlopen_response(body):
    encoded = json.dumps(body).encode("utf-8")
    mock_resp = MagicMock()
    mock_resp.read.return_value = encoded
    mock_resp.__enter__ = lambda s: s
    mock_resp.__exit__ = MagicMock(return_value=False)
    return mock_resp

def _load_handler(lambda_dir: str, alias: str, env: dict | None = None):
    path = os.path.join(REPO_ROOT, "lambdas", lambda_dir, "handler.py")
    spec = importlib.util.spec_from_file_location(alias, path)
    mod  = importlib.util.module_from_spec(spec)
    sys.modules[alias] = mod
    with patch.dict(os.environ, env or {}):
        spec.loader.exec_module(mod)
    return mod

_my_source = _load_handler("my-source-search", "_my_source_handler")

class TestMySourceSearch:
    def test_happy_path(self):
        mock_response = {"results": [{"id": "1", "title": "Example"}]}
        with patch("urllib.request.urlopen",
                   return_value=_make_urlopen_response(mock_response)):
            ctx = MagicMock()
            ctx.client_context.custom = {"bedrockAgentCoreToolName": "t___my_source_search"}
            result = _my_source.handler({"query": "machine learning"}, ctx)
        assert result["source_type"] == "my_source"
        assert result["count"] >= 1

    def test_empty_query_returns_error(self):
        result = _my_source.handler({}, MagicMock())
        assert "error" in result
```

For **raw bytes** responses (e.g. XML from arXiv):

```python
def _make_raw_response(raw_bytes: bytes):
    mock_resp = MagicMock()
    mock_resp.read.return_value = raw_bytes
    mock_resp.__enter__ = lambda s: s
    mock_resp.__exit__ = MagicMock(return_value=False)
    return mock_resp
```

---

## Lambda + CDK Wiring

### 1. Create the Lambda directory

```
lambdas/my-source-search/
└── handler.py
```

### 2. Add the CDK Lambda construct

In `stacks/open_data_stack.py`, after the last similar construct:

```python
my_source_search_fn = lambda_.Function(
    self, "MySourceSearch",
    function_name=f"{prefix}-my-source-search",
    runtime=lambda_.Runtime.PYTHON_3_12,
    handler="handler.handler",
    code=lambda_.Code.from_asset("lambdas/my-source-search"),
    timeout=Duration.seconds(30),
    memory_size=128,
)
```

If the Lambda needs an environment variable:

```python
my_source_search_fn = lambda_.Function(
    ...,
    environment={"MY_API_KEY": self.node.try_get_context("my_api_key") or ""},
)
```

### 3. Add to permission and KMS lists

```python
_new_tool_fns = [
    ...,
    my_source_search_fn,   # AgentCore invoke permission
]

_all_lambda_fns = [
    ...,
    my_source_search_fn,   # KMS grant (when enable_kms=true)
]
```

### 4. Add to `tool_arns`

```python
tool_arns = {
    ...,
    "my_source_search": my_source_search_fn.function_arn,
}
```

The `ToolArns` CloudFormation output will include it automatically.

### 5. Add a federated search dispatch function

In `lambdas/federated-search/handler.py`, add a `_search_my_source` function
(10–15 lines) before the `handler()` function, then register it in `_search_fn`:

```python
def _search_my_source(query_words: list, source: dict) -> list:
    query = " ".join(query_words)
    if not query:
        return []
    # ... call the API, filter by keyword match, return federated-format records
    results = []
    for item in api_results:
        text = item.get("title", "").lower()
        matches = sum(1 for w in query_words if w in text)
        score = min(matches / len(query_words), 1.0) if query_words else 0.0
        if score > 0:
            results.append({
                "source_id": source.get("source_id", f"my_source/{item['id']}"),
                "source_type": "my_source",
                "display_name": item.get("title", ""),
                "match_score": score,
                "description": item.get("abstract", "")[:200],
                "quality_score": None,
            })
    return results

# In handler():
_search_fn = {
    ...,
    "my_source": _search_my_source,
}
```

---

## Brief Example

See `lambdas/pubmed-search/handler.py` for a two-step API pattern (esearch →
esummary). See `lambdas/biorxiv-search/handler.py` for a single-call pattern
with keyword filtering. See `lambdas/reagent-search/handler.py` for the
optional-API-key stub pattern.
