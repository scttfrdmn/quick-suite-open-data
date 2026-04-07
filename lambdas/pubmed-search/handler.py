"""
pubmed_search: Search PubMed literature via NCBI E-utilities API.

AgentCore Lambda target — invoked directly by the Gateway.
Event dict contains tool arguments. Returns a plain dict.

Public API — no authentication required (optional NCBI_API_KEY env var
raises rate limit from 3 to 10 requests/second).
Uses urllib.request + stdlib only. Two-step: esearch then esummary.
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

_ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
_ESUMMARY_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
_MAX_RESULTS = 50
_CURRENT_YEAR = datetime.now(timezone.utc).year

NCBI_API_KEY = os.environ.get("NCBI_API_KEY", "")


def _recency_score(pubdate: str) -> float:
    """Return a recency quality score based on the publication year."""
    try:
        year = int(pubdate[:4])
    except (ValueError, TypeError, IndexError):
        return 0.3
    if year >= _CURRENT_YEAR:
        return 1.0
    if year >= _CURRENT_YEAR - 1:
        return 0.8
    if year >= _CURRENT_YEAR - 2:
        return 0.6
    return 0.3


def _esearch(query: str, max_results: int, date_start: str | None, date_end: str | None) -> list:
    """Call esearch and return a list of PMIDs."""
    params: dict = {
        "db": "pubmed",
        "term": query,
        "retmax": min(max_results, _MAX_RESULTS),
        "retmode": "json",
    }
    if NCBI_API_KEY:
        params["api_key"] = NCBI_API_KEY
    if date_start or date_end:
        params["datetype"] = "pdat"
        if date_start:
            params["mindate"] = date_start
        if date_end:
            params["maxdate"] = date_end

    url = _ESEARCH_URL + "?" + urllib.parse.urlencode(params)
    try:
        req = urllib.request.Request(
            url, headers={"Accept": "application/json", "User-Agent": "quick-suite-data/1.0"}
        )
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        logger.warning(json.dumps({"pubmed_esearch_http_error": e.code}))
        return []
    except Exception as e:
        logger.warning(json.dumps({"pubmed_esearch_error": str(e)}))
        return []

    return data.get("esearchresult", {}).get("idlist", [])


def _esummary(pmids: list) -> dict:
    """Call esummary for a list of PMIDs and return the result dict keyed by uid."""
    if not pmids:
        return {}

    params: dict = {
        "db": "pubmed",
        "id": ",".join(pmids),
        "retmode": "json",
        "version": "2.0",
    }
    if NCBI_API_KEY:
        params["api_key"] = NCBI_API_KEY

    url = _ESUMMARY_URL + "?" + urllib.parse.urlencode(params)
    try:
        req = urllib.request.Request(
            url, headers={"Accept": "application/json", "User-Agent": "quick-suite-data/1.0"}
        )
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        logger.warning(json.dumps({"pubmed_esummary_http_error": e.code}))
        return {}
    except Exception as e:
        logger.warning(json.dumps({"pubmed_esummary_error": str(e)}))
        return {}

    return data.get("result", {})


def _search_pubmed(
    query: str,
    max_results: int,
    date_start: str | None,
    date_end: str | None,
    pub_type_filter: str | None,
) -> list:
    """Run the two-step esearch → esummary flow and return normalized records."""
    pmids = _esearch(query, max_results, date_start, date_end)
    if not pmids:
        return []

    result_data = _esummary(pmids)
    if not result_data:
        return []

    # The result dict has a "uids" list plus one key per uid
    uids = result_data.get("uids", pmids)

    records = []
    for uid in uids:
        item = result_data.get(str(uid))
        if not item or not isinstance(item, dict):
            continue

        pub_types = [
            pt.get("value", "") for pt in (item.get("pubtype") or [])
            if isinstance(pt, dict)
        ]
        if pub_type_filter and pub_type_filter.lower() not in [pt.lower() for pt in pub_types]:
            continue

        pmid = str(item.get("uid", uid))
        title = item.get("title", "")
        authors_raw = item.get("authors") or []
        authors = [a.get("name", "") for a in authors_raw if isinstance(a, dict) and a.get("name")]
        journal = item.get("source", "")
        pubdate = item.get("pubdate") or item.get("epubdate") or ""

        records.append({
            "pmid": pmid,
            "title": title,
            "authors": authors,
            "journal": journal,
            "pub_date": pubdate,
            "abstract_text": "",
            "mesh_terms": [],
            "quality_score": _recency_score(pubdate),
            "source_type": "pubmed",
            "source_id": f"pubmed/{pmid}",
        })

    return records[:max_results]


def handler(event: dict, context: Any) -> dict:
    """
    Search PubMed literature via NCBI E-utilities API.

    Tool arguments:
    - query: str (required) — keyword search terms
    - date_start: str (optional) — YYYY/MM/DD lower bound on publication date
    - date_end: str (optional) — YYYY/MM/DD upper bound on publication date
    - pub_type_filter: str (optional) — filter by publication type (e.g. "Journal Article")
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

    date_start = (event.get("date_start") or "").strip() or None
    date_end = (event.get("date_end") or "").strip() or None
    pub_type_filter = (event.get("pub_type_filter") or "").strip() or None

    try:
        max_results = int(event.get("max_results", 20))
    except (TypeError, ValueError):
        max_results = 20
    max_results = min(max(1, max_results), _MAX_RESULTS)

    results = _search_pubmed(query, max_results, date_start, date_end, pub_type_filter)

    return {
        "source_type": "pubmed",
        "query": query,
        "results": results,
        "count": len(results),
    }
