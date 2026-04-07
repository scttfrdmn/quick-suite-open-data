"""
semantic_scholar_search: Search academic papers via Semantic Scholar Graph API.

AgentCore Lambda target — invoked directly by the Gateway.
Event dict contains tool arguments. Returns a plain dict.

Public API — no authentication required (optional SEMANTIC_SCHOLAR_API_KEY env
var increases rate limits).
Uses urllib.request + stdlib only.
"""

import json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_BASE_URL = "https://api.semanticscholar.org/graph/v1/paper/search"
_FIELDS = "paperId,title,authors,year,citationCount,fieldsOfStudy,abstract"
_MAX_RESULTS = 50
_ABSTRACT_MAX_CHARS = 500
_CURRENT_YEAR = datetime.now(timezone.utc).year

SEMANTIC_SCHOLAR_API_KEY = os.environ.get("SEMANTIC_SCHOLAR_API_KEY", "")


def _recency_score(year: int | None) -> float:
    """Return a recency quality score based on publication year."""
    if year is None:
        return 0.3
    if year >= _CURRENT_YEAR:
        return 1.0
    if year >= _CURRENT_YEAR - 1:
        return 0.8
    if year >= _CURRENT_YEAR - 2:
        return 0.6
    return 0.3


def _quality_score(citation_count: int, year: int | None) -> float:
    """Blend citation popularity and recency into a 0–1 quality score."""
    citation_component = min(1.0, citation_count / 100) * 0.6
    recency_component = _recency_score(year) * 0.4
    return round(citation_component + recency_component, 3)


def _search_semantic_scholar(
    query: str,
    max_results: int,
    fields_of_study: str | None,
    year_start: int | None,
    year_end: int | None,
    min_citations: int,
) -> list:
    """Call Semantic Scholar Graph API and return normalized records."""
    # Fetch more than needed so client-side filters can cull
    fetch_limit = min(max_results * 3, 100)
    params = {
        "query": query,
        "fields": _FIELDS,
        "limit": fetch_limit,
    }
    url = _BASE_URL + "?" + urllib.parse.urlencode(params)

    headers: dict = {
        "Accept": "application/json",
        "User-Agent": "quick-suite-data/1.0",
    }
    if SEMANTIC_SCHOLAR_API_KEY:
        headers["x-api-key"] = SEMANTIC_SCHOLAR_API_KEY

    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        logger.warning(json.dumps({"semantic_scholar_http_error": e.code}))
        return []
    except Exception as e:
        logger.warning(json.dumps({"semantic_scholar_error": str(e)}))
        return []

    items = data.get("data", [])
    results = []
    for item in items:
        year = item.get("year")
        citation_count = item.get("citationCount") or 0
        fos_list = item.get("fieldsOfStudy") or []

        # Client-side filters
        if min_citations and citation_count < min_citations:
            continue
        if year_start and year and year < year_start:
            continue
        if year_end and year and year > year_end:
            continue
        if fields_of_study and not any(
            fields_of_study.lower() in f.lower() for f in (fos_list or [])
        ):
            continue

        paper_id = item.get("paperId", "")
        title = item.get("title", "")
        authors_raw = item.get("authors") or []
        authors = [a.get("name", "") for a in authors_raw if isinstance(a, dict)]
        abstract = (item.get("abstract") or "")[:_ABSTRACT_MAX_CHARS]

        results.append({
            "paper_id": paper_id,
            "title": title,
            "authors": authors,
            "year": year,
            "citation_count": citation_count,
            "fields_of_study": fos_list,
            "abstract": abstract,
            "quality_score": _quality_score(citation_count, year),
            "source_type": "semantic_scholar",
            "source_id": f"s2/{paper_id}",
        })

        if len(results) >= max_results:
            break

    return results


def handler(event: dict, context: Any) -> dict:
    """
    Search academic papers via Semantic Scholar Graph API.

    Tool arguments:
    - query: str (required) — keyword search terms
    - fields_of_study: str (optional) — filter by field of study (partial match)
    - year_start: int (optional) — earliest publication year (inclusive)
    - year_end: int (optional) — latest publication year (inclusive)
    - min_citations: int (optional) — minimum citation count filter
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

    fields_of_study = (event.get("fields_of_study") or "").strip() or None

    year_start: int | None = None
    if event.get("year_start") is not None:
        try:
            year_start = int(event["year_start"])
        except (TypeError, ValueError):
            return {"error": "year_start must be an integer"}

    year_end: int | None = None
    if event.get("year_end") is not None:
        try:
            year_end = int(event["year_end"])
        except (TypeError, ValueError):
            return {"error": "year_end must be an integer"}

    try:
        min_citations = int(event.get("min_citations", 0))
    except (TypeError, ValueError):
        min_citations = 0

    try:
        max_results = int(event.get("max_results", 20))
    except (TypeError, ValueError):
        max_results = 20
    max_results = min(max(1, max_results), _MAX_RESULTS)

    results = _search_semantic_scholar(
        query, max_results, fields_of_study, year_start, year_end, min_citations
    )

    return {
        "source_type": "semantic_scholar",
        "query": query,
        "results": results,
        "count": len(results),
    }
