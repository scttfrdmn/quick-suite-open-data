"""
arxiv_search: Search arXiv preprints via the arXiv Atom API.

AgentCore Lambda target — invoked directly by the Gateway.
Event dict contains tool arguments. Returns a plain dict.

Public API — no authentication required.
Uses urllib.request + xml.etree.ElementTree + stdlib only.

NOTE: arXiv ToS requires no more than one request per 3 seconds from
automated clients. Callers in production should rate-limit accordingly;
this Lambda does not sleep between calls.
"""

import json
import logging
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from typing import Any

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_BASE_URL = "http://export.arxiv.org/api/query"
_ATOM_NS = "{http://www.w3.org/2005/Atom}"
_MAX_RESULTS = 50
_SUMMARY_MAX_CHARS = 500


def _parse_arxiv_atom(xml_bytes: bytes) -> list:
    """Parse an arXiv Atom XML response and return a list of entry dicts."""
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as e:
        logger.warning(json.dumps({"arxiv_xml_parse_error": str(e)}))
        return []

    entries = []
    for entry in root.findall(f"{_ATOM_NS}entry"):
        # Extract arxiv_id from the <id> URL, e.g. http://arxiv.org/abs/2301.00001v1
        id_text = (entry.findtext(f"{_ATOM_NS}id") or "").strip()
        arxiv_id = id_text.split("/abs/")[-1] if "/abs/" in id_text else id_text

        title_el = entry.find(f"{_ATOM_NS}title")
        title = (title_el.text or "").strip() if title_el is not None else ""

        authors = []
        for author_el in entry.findall(f"{_ATOM_NS}author"):
            name_el = author_el.find(f"{_ATOM_NS}name")
            if name_el is not None and name_el.text:
                authors.append(name_el.text.strip())

        published_el = entry.find(f"{_ATOM_NS}published")
        published = (published_el.text or "").strip() if published_el is not None else ""

        summary_el = entry.find(f"{_ATOM_NS}summary")
        summary = (summary_el.text or "").strip() if summary_el is not None else ""
        summary = summary[:_SUMMARY_MAX_CHARS]

        categories = [
            cat.get("term", "")
            for cat in entry.findall(f"{_ATOM_NS}category")
            if cat.get("term")
        ]

        entries.append({
            "arxiv_id": arxiv_id,
            "title": title,
            "authors": authors,
            "published": published,
            "summary": summary,
            "categories": categories,
            "source_type": "arxiv",
            "source_id": f"arxiv/{arxiv_id}",
        })

    return entries


def _search_arxiv(
    query: str,
    max_results: int,
    date_start: str | None,
    date_end: str | None,
    category_filter: str | None,
) -> list:
    """Call arXiv API and return parsed, filtered records."""
    encoded_query = urllib.parse.quote_plus(query)
    url = f"{_BASE_URL}?search_query=all:{encoded_query}&max_results={max_results}"
    if date_start or date_end:
        ds = date_start or "0"
        de = date_end or "99999999999999"
        url += f"&submittedDate=[{ds}+TO+{de}]"

    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "quick-suite-data/1.0"}
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            xml_bytes = resp.read()
    except urllib.error.HTTPError as e:
        logger.warning(json.dumps({"arxiv_http_error": e.code}))
        return []
    except Exception as e:
        logger.warning(json.dumps({"arxiv_error": str(e)}))
        return []

    entries = _parse_arxiv_atom(xml_bytes)

    if category_filter:
        entries = [e for e in entries if category_filter in e["categories"]]

    return entries[:max_results]


def handler(event: dict, context: Any) -> dict:
    """
    Search arXiv preprints via the arXiv Atom API.

    Tool arguments:
    - query: str (required) — keyword search terms
    - category_filter: str (optional) — filter results to entries with this category
      (e.g. "cs.LG", "physics.comp-ph"); applied client-side after fetch
    - date_start: str (optional) — YYYYMMDDHHMMSS lower bound (arXiv submittedDate format)
    - date_end: str (optional) — YYYYMMDDHHMMSS upper bound
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

    category_filter = (event.get("category_filter") or "").strip() or None
    date_start = (event.get("date_start") or "").strip() or None
    date_end = (event.get("date_end") or "").strip() or None

    try:
        max_results = int(event.get("max_results", 20))
    except (TypeError, ValueError):
        max_results = 20
    max_results = min(max(1, max_results), _MAX_RESULTS)

    results = _search_arxiv(query, max_results, date_start, date_end, category_filter)

    return {
        "source_type": "arxiv",
        "query": query,
        "results": results,
        "count": len(results),
    }
