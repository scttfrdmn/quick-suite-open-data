"""
research_search: Search Zenodo and Figshare for research datasets.

AgentCore Lambda target — invoked directly by the Gateway.
Event dict contains tool arguments. Returns a plain dict.

Public APIs — no authentication required.
Uses urllib.request + stdlib only.
"""

import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ZENODO_API = "https://zenodo.org/api/records"
FIGSHARE_API = "https://api.figshare.com/v2/articles/search"

_MAX_RESULTS = 50


def _fetch_with_retry(
    url: str,
    data: bytes | None = None,
    method: str = "GET",
    headers: dict | None = None,
    max_retries: int = 3,
) -> dict | None:
    """Fetch URL with exponential backoff on 429."""
    for attempt in range(max_retries):
        try:
            req = urllib.request.Request(
                url, data=data, method=method, headers=headers or {}
            )
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < max_retries - 1:
                time.sleep(2**attempt)
                continue
            logger.warning("HTTP error %s fetching %s", e.code, url)
            return None
        except Exception as exc:
            logger.warning("Error fetching %s: %s", url, exc)
            return None
    return None


def _search_zenodo(query: str, max_results: int) -> list[dict]:
    """Search Zenodo public records."""
    params = urllib.parse.urlencode(
        {"q": query, "size": max_results, "type": "dataset"}
    )
    url = f"{ZENODO_API}?{params}"
    data = _fetch_with_retry(url)
    if not data or "hits" not in data:
        return []
    results = []
    for hit in data.get("hits", {}).get("hits", [])[:max_results]:
        meta = hit.get("metadata", {})
        files = hit.get("files", [])
        results.append(
            {
                "source_type": "zenodo",
                "source_id": f"zenodo/{hit.get('id', '')}",
                "display_name": meta.get("title", ""),
                "description": (meta.get("description", "") or "")[:200],
                "doi": meta.get("doi", ""),
                "created_at": meta.get("publication_date", ""),
                "url": hit.get("links", {}).get("self", ""),
                "download_url": (
                    files[0].get("links", {}).get("self", "") if files else ""
                ),
            }
        )
    return results


def _search_figshare(query: str, max_results: int) -> list[dict]:
    """Search Figshare public articles."""
    body = json.dumps(
        {"search_for": query, "page_size": max_results, "item_type": 3}
    ).encode()
    data = _fetch_with_retry(
        FIGSHARE_API,
        data=body,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    if not data or not isinstance(data, list):
        return []
    results = []
    for item in data[:max_results]:
        results.append(
            {
                "source_type": "figshare",
                "source_id": f"figshare/{item.get('id', '')}",
                "display_name": item.get("title", ""),
                "description": (item.get("description", "") or "")[:200],
                "doi": item.get("doi", ""),
                "created_at": item.get("published_date", ""),
                "url": item.get("url_public_html", ""),
                "download_url": "",
            }
        )
    return results


def handler(event: dict, context: Any = None) -> dict:
    """AgentCore Lambda target: search Zenodo and Figshare."""
    _tool_name = "unknown"
    try:
        raw = context.client_context.custom["bedrockAgentCoreToolName"]
        _tool_name = raw.split("___")[-1]
    except Exception:
        pass
    logger.info(json.dumps({"tool": _tool_name, "event": event}))

    query = (event.get("query") or "").strip()
    if not query:
        return {"error": "query is required", "results": []}

    sources = event.get("sources", ["zenodo", "figshare"])
    try:
        max_results = int(event.get("max_results", 20))
    except (TypeError, ValueError):
        max_results = 20
    max_results = min(max(1, max_results), _MAX_RESULTS)

    results = []
    skipped = []

    if "zenodo" in sources:
        try:
            results.extend(_search_zenodo(query, max_results))
        except Exception as exc:
            logger.warning("Zenodo search failed: %s", exc)
            skipped.append("zenodo")

    if "figshare" in sources:
        try:
            results.extend(_search_figshare(query, max_results))
        except Exception as exc:
            logger.warning("Figshare search failed: %s", exc)
            skipped.append("figshare")

    # Sort by display_name
    results.sort(key=lambda r: r.get("display_name", "").lower())

    return {
        "results": results[:max_results],
        "total": len(results),
        "skipped_sources": skipped,
    }
