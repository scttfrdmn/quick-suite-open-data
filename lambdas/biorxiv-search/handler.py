"""
biorxiv_search: Search bioRxiv and/or medRxiv preprints via bioRxiv API.

AgentCore Lambda target — invoked directly by the Gateway.
Event dict contains tool arguments. Returns a plain dict.

Public API — no authentication required.
Uses urllib.request + stdlib only.
"""

import json
import logging
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_BASE_URL = "https://api.biorxiv.org/details"
_MAX_RESULTS = 50
_ABSTRACT_MAX_CHARS = 500

_VALID_SERVERS = {"biorxiv", "medrxiv", "both"}

# Today per project environment — 2026-04-07
_TODAY = datetime.now(timezone.utc).date()
_DEFAULT_START = (_TODAY - timedelta(days=30)).strftime("%Y-%m-%d")
_DEFAULT_END = _TODAY.strftime("%Y-%m-%d")


def _fetch_server(server: str, date_start: str, date_end: str, max_results: int) -> list:
    """Fetch preprints from one server (biorxiv or medrxiv)."""
    interval = f"{date_start}/{date_end}"
    url = f"{_BASE_URL}/{server}/{interval}/0/json"
    try:
        req = urllib.request.Request(
            url, headers={"Accept": "application/json", "User-Agent": "quick-suite-data/1.0"}
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        logger.warning(json.dumps({"biorxiv_http_error": e.code, "server": server}))
        return []
    except Exception as e:
        logger.warning(json.dumps({"biorxiv_error": str(e), "server": server}))
        return []

    collection = data.get("collection", [])
    records = []
    for item in collection[:max_results]:
        doi = item.get("doi", "")
        records.append({
            "doi": doi,
            "title": item.get("title", ""),
            "authors": item.get("authors", ""),
            "category": item.get("category", ""),
            "date": item.get("date", ""),
            "abstract": (item.get("abstract") or "")[:_ABSTRACT_MAX_CHARS],
            "source_type": server,
            "source_id": f"{server}/{doi}",
        })
    return records


def _keyword_match(query: str, record: dict) -> bool:
    """Return True if any query word appears in the title or abstract."""
    words = [w.lower() for w in query.split() if w]
    if not words:
        return True
    text = (record.get("title", "") + " " + record.get("abstract", "")).lower()
    return any(w in text for w in words)


def handler(event: dict, context: Any) -> dict:
    """
    Search bioRxiv / medRxiv preprints via bioRxiv public API.

    Tool arguments:
    - query: str (required) — keyword filter on title and abstract
    - server: str (optional) — biorxiv | medrxiv | both (default: biorxiv)
    - date_start: str (optional) — YYYY-MM-DD (default: 30 days ago)
    - date_end: str (optional) — YYYY-MM-DD (default: today)
    - max_results: int (optional, default 20, max 50)
    """
    _tool_name = "unknown"
    try:
        raw = context.client_context.custom["bedrockAgentCoreToolName"]
        _tool_name = raw.split("___")[-1]
    except Exception:
        pass
    logger.info(json.dumps({"tool": _tool_name, "event": event}))

    query = (event.get("query") or "").strip()
    if not query:
        return {"error": "query is required"}

    server = (event.get("server") or "biorxiv").strip().lower()
    if server not in _VALID_SERVERS:
        return {"error": f"server must be one of: {', '.join(sorted(_VALID_SERVERS))}"}

    date_start = (event.get("date_start") or _DEFAULT_START).strip()
    date_end = (event.get("date_end") or _DEFAULT_END).strip()

    try:
        max_results = int(event.get("max_results", 20))
    except (TypeError, ValueError):
        max_results = 20
    max_results = min(max(1, max_results), _MAX_RESULTS)

    raw_records: list = []
    if server == "both":
        raw_records.extend(_fetch_server("biorxiv", date_start, date_end, max_results))
        raw_records.extend(_fetch_server("medrxiv", date_start, date_end, max_results))
    else:
        raw_records = _fetch_server(server, date_start, date_end, max_results)

    # Filter by keyword match
    results = [r for r in raw_records if _keyword_match(query, r)][:max_results]

    return {
        "source_type": "biorxiv",
        "query": query,
        "results": results,
        "count": len(results),
    }
