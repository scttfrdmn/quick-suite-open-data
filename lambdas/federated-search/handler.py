"""
federated_search: Search across all registered data sources.

AgentCore Lambda target — invoked directly by the Gateway.
Event dict contains tool arguments. Returns a plain dict.

Reads sources from qs-data-source-registry, queries each source type,
merges and ranks results by keyword match score.
"""

import json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

import boto3

# ---------------------------------------------------------------------------
# Data classification clearance ordering (lowest → highest)
# ---------------------------------------------------------------------------

_CLEARANCE_LEVELS = {"public": 0, "internal": 1, "restricted": 2, "phi": 3}

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")

REGISTRY_TABLE = os.environ.get("REGISTRY_TABLE", "")
CATALOG_TABLE = os.environ.get("CATALOG_TABLE", "")
SNOWFLAKE_SECRET_ARN = os.environ.get("SNOWFLAKE_SECRET_ARN", "")
REDSHIFT_SECRET_ARN = os.environ.get("REDSHIFT_SECRET_ARN", "")

secrets_client = boto3.client("secretsmanager")
redshift_data = boto3.client("redshift-data")


# ---------------------------------------------------------------------------
# Keyword scoring
# ---------------------------------------------------------------------------

def _keyword_score(query_words: list, display_name: str, description: str) -> float:
    """Count query word occurrences in display_name + description, capped at 1.0."""
    if not query_words:
        return 0.0
    text = (display_name + " " + description).lower()
    matches = sum(1 for w in query_words if w in text)
    return min(matches / len(query_words), 1.0)


# ---------------------------------------------------------------------------
# Source-type search functions
# ---------------------------------------------------------------------------

def _search_roda(query_words: list, source: dict) -> list:
    """Search RODA catalog table by keyword match on searchText."""
    if not CATALOG_TABLE:
        return []

    table = dynamodb.Table(CATALOG_TABLE)
    try:
        resp = table.scan(Limit=500)
        items = resp.get("Items", [])
    except Exception as e:
        logger.warning(json.dumps({"roda_scan_error": str(e)}))
        raise

    results = []
    for item in items:
        search_text = (item.get("searchText") or "").lower()
        matches = sum(1 for w in query_words if w in search_text) if query_words else 0
        score = min(matches / len(query_words), 1.0) if query_words else 0.0
        if score > 0:
            results.append({
                "source_id": source["source_id"],
                "source_type": "roda",
                "display_name": item.get("name", source.get("display_name", "")),
                "match_score": score,
                "description": (item.get("description") or "")[:200],
                "quality_score": item.get("quality_score"),
            })
    return results


def _search_s3(query_words: list, source: dict) -> list:
    """Match S3 source display_name and description against query words."""
    display_name = source.get("display_name", "")
    description = source.get("description", "")
    score = _keyword_score(query_words, display_name, description)
    if score <= 0:
        return []
    return [{
        "source_id": source["source_id"],
        "source_type": "s3",
        "display_name": display_name,
        "match_score": score,
        "description": description[:200],
        "quality_score": None,
    }]


def _get_secret(secret_arn: str) -> dict | None:
    if not secret_arn:
        return None
    try:
        resp = secrets_client.get_secret_value(SecretId=secret_arn)
        return json.loads(resp["SecretString"])
    except Exception as e:
        logger.warning(json.dumps({"secret_error": str(e)}))
        return None


def _search_snowflake(query_words: list, source: dict) -> list:
    """List Snowflake tables and match names against query words."""
    import base64
    import urllib.request

    config = _get_secret(SNOWFLAKE_SECRET_ARN)
    if not config:
        raise RuntimeError("Snowflake not configured")

    account = config["account"]
    user = config["user"]
    password = config["password"]
    warehouse = config.get("warehouse", "")
    role = config.get("role", "")
    database = config.get("database", "")

    url = f"https://{account}.snowflakecomputing.com/api/v2/statements"
    credentials = base64.b64encode(f"{user}:{password}".encode()).decode()
    sql = (
        "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
        "WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_SCHEMA, TABLE_NAME"
    )
    body_bytes = json.dumps({
        "statement": sql,
        "warehouse": warehouse,
        "role": role,
        "database": database,
    }).encode("utf-8")

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
        result = json.loads(resp.read().decode("utf-8"))

    rows = result.get("data", [])
    results = []
    for row in rows:
        table_name = (row[1] if len(row) > 1 else "").lower()
        schema_name = (row[0] if row else "").lower()
        combined = f"{schema_name} {table_name}"
        score = _keyword_score(query_words, combined, "")
        if score > 0:
            results.append({
                "source_id": source["source_id"],
                "source_type": "snowflake",
                "display_name": f"{source.get('display_name', '')} / {row[0]}.{row[1]}",
                "match_score": score,
                "description": source.get("description", "")[:200],
                "quality_score": None,
            })
    return results


def _search_ipeds(query_words: list, source: dict) -> list:
    """Search IPEDS via Urban Institute Education Data Portal (public API, no auth)."""
    query = " ".join(query_words)
    if not query:
        return []

    params = {"keyword": query, "page[size]": 20}
    url = "https://educationdata.urban.org/api/v1/college-university/ipeds/variables/?" + urllib.parse.urlencode(params)
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": "quick-suite-data/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        logger.warning(json.dumps({"ipeds_error": str(e)}))
        raise

    items = data if isinstance(data, list) else data.get("results", data.get("data", []))
    results = []
    for item in items[:20]:
        var_name = item.get("varTitle") or item.get("varname") or item.get("label") or ""
        description = item.get("definition") or item.get("description") or ""
        text = (var_name + " " + description).lower()
        matches = sum(1 for w in query_words if w in text)
        score = min(matches / len(query_words), 1.0) if query_words else 0.0
        if score > 0:
            results.append({
                "source_id": source.get("source_id", f"ipeds/{var_name}"),
                "source_type": "ipeds",
                "display_name": var_name or "IPEDS variable",
                "match_score": score,
                "description": description[:200],
                "quality_score": None,
            })
    return results


def _search_nih_reporter(query_words: list, source: dict) -> list:
    """Search NIH Reporter v2 API (public API, no auth)."""
    query = " ".join(query_words)
    if not query:
        return []

    body = {
        "criteria": {"text_search": {"operator": "and", "search_field": "all", "terms": query}},
        "limit": 20, "offset": 0,
        "fields": ["ProjectNum", "ProjectTitle", "PiNames", "FiscalYear", "AwardAmount", "AbstractText"],
    }
    body_bytes = json.dumps(body).encode("utf-8")
    try:
        req = urllib.request.Request(
            "https://api.reporter.nih.gov/v2/projects/search",
            data=body_bytes, method="POST",
            headers={"Content-Type": "application/json", "Accept": "application/json", "User-Agent": "quick-suite-data/1.0"},
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        logger.warning(json.dumps({"nih_reporter_error": str(e)}))
        raise

    results = []
    for item in data.get("results", [])[:20]:
        title = item.get("ProjectTitle") or ""
        abstract = (item.get("AbstractText") or "")[:200]
        text = (title + " " + abstract).lower()
        matches = sum(1 for w in query_words if w in text)
        score = min(matches / len(query_words), 1.0) if query_words else 0.0
        if score > 0:
            results.append({
                "source_id": source.get("source_id", f"nih/{item.get('ProjectNum', '')}"),
                "source_type": "nih_reporter",
                "display_name": title,
                "match_score": score,
                "description": abstract,
                "quality_score": None,
            })
    return results


def _search_nsf_awards(query_words: list, source: dict) -> list:
    """Search NSF Award Search API (public API, no auth)."""
    query = " ".join(query_words)
    if not query:
        return []

    params = {"keyword": query, "printFields": "id,title,piFirstName,piLastName,awardeeName,abstractText", "rows": 20}
    url = "https://api.nsf.gov/services/v1/awards.json?" + urllib.parse.urlencode(params)
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": "quick-suite-data/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        logger.warning(json.dumps({"nsf_error": str(e)}))
        raise

    results = []
    for item in data.get("response", {}).get("award", [])[:20]:
        title = item.get("title") or ""
        abstract = (item.get("abstractText") or "")[:200]
        text = (title + " " + abstract).lower()
        matches = sum(1 for w in query_words if w in text)
        score = min(matches / len(query_words), 1.0) if query_words else 0.0
        if score > 0:
            results.append({
                "source_id": source.get("source_id", f"nsf/{item.get('id', '')}"),
                "source_type": "nsf_awards",
                "display_name": title,
                "match_score": score,
                "description": abstract,
                "quality_score": None,
            })
    return results


def _search_pubmed(query_words: list, source: dict) -> list:
    """Search PubMed via NCBI E-utilities (public API, no auth)."""
    query = " ".join(query_words)
    if not query:
        return []

    params = {"db": "pubmed", "term": query, "retmax": 20, "retmode": "json"}
    _eutils = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
    esearch_url = f"{_eutils}/esearch.fcgi?" + urllib.parse.urlencode(params)
    _ua_headers = {"Accept": "application/json", "User-Agent": "quick-suite-data/1.0"}
    try:
        req = urllib.request.Request(esearch_url, headers=_ua_headers)
        with urllib.request.urlopen(req, timeout=20) as resp:
            esearch_data = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        logger.warning(json.dumps({"pubmed_esearch_error": str(e)}))
        raise

    pmids = esearch_data.get("esearchresult", {}).get("idlist", [])
    if not pmids:
        return []

    sum_params = {"db": "pubmed", "id": ",".join(pmids[:20]), "retmode": "json", "version": "2.0"}
    esummary_url = f"{_eutils}/esummary.fcgi?" + urllib.parse.urlencode(sum_params)
    try:
        req2 = urllib.request.Request(esummary_url, headers=_ua_headers)
        with urllib.request.urlopen(req2, timeout=20) as resp2:
            sum_data = json.loads(resp2.read().decode("utf-8"))
    except Exception as e:
        logger.warning(json.dumps({"pubmed_esummary_error": str(e)}))
        raise

    result_data = sum_data.get("result", {})
    uids = result_data.get("uids", pmids)
    results = []
    for uid in uids[:20]:
        item = result_data.get(str(uid))
        if not item or not isinstance(item, dict):
            continue
        title = item.get("title", "")
        text = title.lower()
        matches = sum(1 for w in query_words if w in text)
        score = min(matches / len(query_words), 1.0) if query_words else 0.0
        if score > 0:
            results.append({
                "source_id": source.get("source_id", f"pubmed/{uid}"),
                "source_type": "pubmed",
                "display_name": title,
                "match_score": score,
                "description": item.get("source", ""),
                "quality_score": None,
            })
    return results


def _search_biorxiv(query_words: list, source: dict) -> list:
    """Search bioRxiv preprints via bioRxiv public API."""
    from datetime import datetime, timedelta
    query = " ".join(query_words)
    if not query:
        return []

    today = datetime.utcnow().date()
    date_start = (today - timedelta(days=30)).strftime("%Y-%m-%d")
    date_end = today.strftime("%Y-%m-%d")
    url = f"https://api.biorxiv.org/details/biorxiv/{date_start}/{date_end}/0/json"
    try:
        req = urllib.request.Request(
            url, headers={"Accept": "application/json", "User-Agent": "quick-suite-data/1.0"}
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        logger.warning(json.dumps({"biorxiv_error": str(e)}))
        raise

    results = []
    for item in data.get("collection", [])[:20]:
        title = item.get("title", "")
        abstract = (item.get("abstract") or "")[:200]
        text = (title + " " + abstract).lower()
        matches = sum(1 for w in query_words if w in text)
        score = min(matches / len(query_words), 1.0) if query_words else 0.0
        if score > 0:
            doi = item.get("doi", "")
            results.append({
                "source_id": source.get("source_id", f"biorxiv/{doi}"),
                "source_type": "biorxiv",
                "display_name": title,
                "match_score": score,
                "description": abstract,
                "quality_score": None,
            })
    return results


def _search_semantic_scholar(query_words: list, source: dict) -> list:
    """Search academic papers via Semantic Scholar Graph API (public API)."""
    query = " ".join(query_words)
    if not query:
        return []

    params = {
        "query": query,
        "fields": "paperId,title,authors,year,citationCount,abstract",
        "limit": 20,
    }
    url = "https://api.semanticscholar.org/graph/v1/paper/search?" + urllib.parse.urlencode(params)
    try:
        req = urllib.request.Request(
            url, headers={"Accept": "application/json", "User-Agent": "quick-suite-data/1.0"}
        )
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        logger.warning(json.dumps({"semantic_scholar_error": str(e)}))
        raise

    results = []
    for item in data.get("data", [])[:20]:
        title = item.get("title", "")
        abstract = (item.get("abstract") or "")[:200]
        text = (title + " " + abstract).lower()
        matches = sum(1 for w in query_words if w in text)
        score = min(matches / len(query_words), 1.0) if query_words else 0.0
        if score > 0:
            paper_id = item.get("paperId", "")
            results.append({
                "source_id": source.get("source_id", f"s2/{paper_id}"),
                "source_type": "semantic_scholar",
                "display_name": title,
                "match_score": score,
                "description": abstract,
                "quality_score": None,
            })
    return results


def _search_arxiv(query_words: list, source: dict) -> list:
    """Search arXiv preprints via the arXiv Atom API."""
    import xml.etree.ElementTree as ET

    query = " ".join(query_words)
    if not query:
        return []

    encoded = urllib.parse.quote_plus(query)
    url = f"http://export.arxiv.org/api/query?search_query=all:{encoded}&max_results=20"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "quick-suite-data/1.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            xml_bytes = resp.read()
    except Exception as e:
        logger.warning(json.dumps({"arxiv_error": str(e)}))
        raise

    ns = "{http://www.w3.org/2005/Atom}"
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as e:
        logger.warning(json.dumps({"arxiv_xml_error": str(e)}))
        return []

    results = []
    for entry in root.findall(f"{ns}entry"):
        title_el = entry.find(f"{ns}title")
        title = (title_el.text or "").strip() if title_el is not None else ""
        summary_el = entry.find(f"{ns}summary")
        summary = (summary_el.text or "").strip() if summary_el is not None else ""
        text = (title + " " + summary[:200]).lower()
        matches = sum(1 for w in query_words if w in text)
        score = min(matches / len(query_words), 1.0) if query_words else 0.0
        if score > 0:
            id_el = entry.find(f"{ns}id")
            id_text = (id_el.text or "").strip() if id_el is not None else ""
            arxiv_id = id_text.split("/abs/")[-1] if "/abs/" in id_text else id_text
            results.append({
                "source_id": source.get("source_id", f"arxiv/{arxiv_id}"),
                "source_type": "arxiv",
                "display_name": title,
                "match_score": score,
                "description": summary[:200],
                "quality_score": None,
            })
    return results


def _search_reagents(query_words: list, source: dict) -> list:
    """Search reagents — returns empty if no ADDGENE_API_KEY configured."""
    import os
    api_key = os.environ.get("ADDGENE_API_KEY", "")
    if not api_key:
        logger.info(json.dumps({"reagent_search": "no_api_key_skipping"}))
        return []

    query = " ".join(query_words)
    params = {"query": query, "page_size": 20}
    url = "https://www.addgene.org/api/v2/plasmids/?" + urllib.parse.urlencode(params)
    try:
        req = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "Authorization": f"Token {api_key}",
                "User-Agent": "quick-suite-data/1.0",
            },
        )
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        logger.warning(json.dumps({"reagent_error": str(e)}))
        raise

    results = []
    items = data.get("results", data if isinstance(data, list) else [])
    for item in items[:20]:
        name = item.get("name", item.get("plasmid_name", ""))
        description = item.get("description", item.get("purpose", ""))
        text = (name + " " + description).lower()
        matches = sum(1 for w in query_words if w in text)
        score = min(matches / len(query_words), 1.0) if query_words else 0.0
        if score > 0:
            reagent_id = str(item.get("id", ""))
            results.append({
                "source_id": source.get("source_id", f"reagent/{reagent_id}"),
                "source_type": "reagents",
                "display_name": name,
                "match_score": score,
                "description": description[:200],
                "quality_score": None,
            })
    return results


def _search_redshift(query_words: list, source: dict) -> list:
    """List Redshift tables and match names against query words."""
    import time

    config = _get_secret(REDSHIFT_SECRET_ARN)
    if not config:
        raise RuntimeError("Redshift not configured")

    workgroup = config.get("workgroup", "")
    database = config.get("database", "")
    secret_arn = config.get("secret_arn", "")

    sql = (
        "SELECT table_schema, table_name FROM information_schema.tables "
        "WHERE table_schema NOT IN ('pg_catalog', 'information_schema') "
        "ORDER BY table_schema, table_name"
    )

    exec_resp = redshift_data.execute_statement(
        WorkgroupName=workgroup,
        Database=database,
        SecretArn=secret_arn,
        Sql=sql,
    )
    statement_id = exec_resp["Id"]

    for _ in range(30):
        desc = redshift_data.describe_statement(Id=statement_id)
        status = desc.get("Status", "")
        if status in ("FINISHED", "FAILED", "ABORTED"):
            break
        time.sleep(1)
    else:
        raise TimeoutError("Redshift query timed out")

    if status in ("FAILED", "ABORTED"):
        raise RuntimeError(f"Redshift query failed: {desc.get('Error', 'unknown')}")

    result = redshift_data.get_statement_result(Id=statement_id)
    records = result.get("Records", [])

    results = []
    for row in records:
        schema_val = (row[0].get("stringValue") or "") if row else ""
        name_val = (row[1].get("stringValue") or "") if len(row) > 1 else ""
        combined = f"{schema_val} {name_val}"
        score = _keyword_score(query_words, combined, "")
        if score > 0:
            results.append({
                "source_id": source["source_id"],
                "source_type": "redshift",
                "display_name": f"{source.get('display_name', '')} / {schema_val}.{name_val}",
                "match_score": score,
                "description": source.get("description", "")[:200],
                "quality_score": None,
            })
    return results


# ---------------------------------------------------------------------------
# Main handler
# ---------------------------------------------------------------------------

def handler(event: dict, context: Any) -> dict:
    """
    Search across all registered data sources.

    Tool arguments:
    - query: str (required)
    - max_results: int (optional, default 10, max 50)
    - data_classification_filter: str (optional) — only return sources with this exact classification
    - caller_clearance: str (optional) — clearance level of the caller; sources above this level
      are excluded. Levels (lowest→highest): public < internal < restricted < phi.
      Defaults to "public" (most restrictive default) if not provided.
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

    try:
        max_results = int(event.get("max_results", 10))
    except (TypeError, ValueError):
        max_results = 10
    max_results = min(max(1, max_results), 50)

    classification_filter = (event.get("data_classification_filter") or "").strip() or None

    # caller_clearance defaults to "public" (most restrictive) if not supplied
    raw_clearance = (event.get("caller_clearance") or "public").strip().lower()
    caller_clearance_level = _CLEARANCE_LEVELS.get(raw_clearance, 0)

    query_words = [w for w in query.lower().split() if w]

    if not REGISTRY_TABLE:
        return {"results": [], "total": 0, "skipped_sources": [], "query": query}

    # Load all sources from registry
    try:
        reg_table = dynamodb.Table(REGISTRY_TABLE)
        resp = reg_table.scan()
        all_sources = resp.get("Items", [])
    except Exception as e:
        logger.error(json.dumps({"registry_scan_error": str(e)}))
        return {"error": f"Failed to load source registry: {e}"}

    # Apply clearance filtering: drop sources whose classification exceeds caller's clearance
    sources = [
        s for s in all_sources
        if _CLEARANCE_LEVELS.get((s.get("data_classification") or "public").lower(), 0)
        <= caller_clearance_level
    ]

    # Apply optional exact classification filter on top of clearance-filtered set
    if classification_filter:
        sources = [s for s in sources if s.get("data_classification") == classification_filter]

    results = []
    skipped_sources = []

    _search_fn = {
        "roda": _search_roda,
        "s3": _search_s3,
        "snowflake": _search_snowflake,
        "redshift": _search_redshift,
        "ipeds": _search_ipeds,
        "nih_reporter": _search_nih_reporter,
        "nsf_awards": _search_nsf_awards,
        "pubmed": _search_pubmed,
        "biorxiv": _search_biorxiv,
        "semantic_scholar": _search_semantic_scholar,
        "arxiv": _search_arxiv,
        "reagents": _search_reagents,
    }

    for source in sources:
        source_id = source.get("source_id", "")
        source_type = source.get("type", "")
        fn = _search_fn.get(source_type)
        if fn is None:
            continue
        try:
            hits = fn(query_words, source)
            results.extend(hits)
        except Exception as e:
            logger.warning(json.dumps({"skipped_source": source_id, "error": str(e)}))
            skipped_sources.append(source_id)

    # Sort by match_score descending, then cap
    results.sort(key=lambda r: r.get("match_score", 0.0), reverse=True)
    results = results[:max_results]

    return {
        "results": results,
        "total": len(results),
        "skipped_sources": skipped_sources,
        "query": query,
    }
