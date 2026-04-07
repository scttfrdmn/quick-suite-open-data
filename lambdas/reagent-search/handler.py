"""
reagent_search: Search biological reagents via Addgene catalog API.

AgentCore Lambda target — invoked directly by the Gateway.
Event dict contains tool arguments. Returns a plain dict.

Requires ADDGENE_API_KEY env var (Addgene registration at addgene.org).
Without an API key, returns an informational note and an empty result set.
Uses urllib.request + stdlib only.
"""

import json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_ADDGENE_API_URL = "https://www.addgene.org/api/v2/plasmids/"
_MAX_RESULTS = 50

ADDGENE_API_KEY = os.environ.get("ADDGENE_API_KEY", "")

_VALID_REAGENT_TYPES = {"plasmid", "cell_line", "bacteria", "all"}


def _search_addgene(
    query: str, reagent_type: str, organism_filter: str | None, max_results: int
) -> list:
    """Call Addgene API v2 and return normalized reagent records."""
    params: dict = {
        "query": query,
        "page_size": min(max_results, _MAX_RESULTS),
    }
    if reagent_type and reagent_type != "all":
        params["type"] = reagent_type
    if organism_filter:
        params["organism"] = organism_filter

    url = _ADDGENE_API_URL + "?" + urllib.parse.urlencode(params)
    try:
        req = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "Authorization": f"Token {ADDGENE_API_KEY}",
                "User-Agent": "quick-suite-data/1.0",
            },
        )
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        logger.warning(json.dumps({"addgene_http_error": e.code}))
        return []
    except Exception as e:
        logger.warning(json.dumps({"addgene_error": str(e)}))
        return []

    items = data.get("results", data if isinstance(data, list) else [])
    results = []
    for item in items[:max_results]:
        reagent_id = str(item.get("id", item.get("addgene_id", "")))
        name = item.get("name", item.get("plasmid_name", ""))
        organism = item.get("organism", item.get("insert_organism", ""))
        description = item.get("description", item.get("purpose", ""))
        catalog_url = item.get("url", f"https://www.addgene.org/{reagent_id}/")
        rtype = item.get("type", reagent_type if reagent_type != "all" else "plasmid")

        results.append({
            "reagent_id": reagent_id,
            "reagent_type": rtype,
            "name": name,
            "organism": organism,
            "description": description,
            "catalog_url": catalog_url,
            "source_type": "reagents",
            "source_id": f"reagent/{reagent_id}",
        })

    return results


def handler(event: dict, context: Any) -> dict:
    """
    Search biological reagents via Addgene catalog API.

    Tool arguments:
    - query: str (required) — keyword search terms
    - reagent_type: str (optional) — plasmid | cell_line | bacteria | all (default: all)
    - organism_filter: str (optional) — filter by organism (e.g. "human", "mouse")
    - max_results: int (optional, default 20, max 50)

    Requires ADDGENE_API_KEY environment variable. Without it, returns an
    informational message and an empty result set.
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

    reagent_type = (event.get("reagent_type") or "all").strip().lower()
    if reagent_type not in _VALID_REAGENT_TYPES:
        return {"error": f"reagent_type must be one of: {', '.join(sorted(_VALID_REAGENT_TYPES))}"}

    organism_filter = (event.get("organism_filter") or "").strip() or None

    try:
        max_results = int(event.get("max_results", 20))
    except (TypeError, ValueError):
        max_results = 20
    max_results = min(max(1, max_results), _MAX_RESULTS)

    if not ADDGENE_API_KEY:
        logger.info(json.dumps({"reagent_search": "no_api_key_configured"}))
        return {
            "source_type": "reagents",
            "query": query,
            "results": [],
            "count": 0,
            "note": (
                "Addgene API requires registration; configure ADDGENE_API_KEY env var "
                "with a token from https://www.addgene.org/api/"
            ),
        }

    results = _search_addgene(query, reagent_type, organism_filter, max_results)

    return {
        "source_type": "reagents",
        "query": query,
        "results": results,
        "count": len(results),
    }
