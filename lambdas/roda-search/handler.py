"""
roda_search: Search the RODA catalog in DynamoDB.

AgentCore Lambda target — invoked directly by the Gateway.
Event dict contains tool arguments. Returns a plain dict.

Supports three search modes:
1. Tag-based (GSI query) — fast, exact match on primary tag
2. Keyword scan — scans searchText field with contains filters
3. Combined — tag filter + keyword refinement
"""

import json
import logging
import os
import re
from typing import Any

import boto3
from boto3.dynamodb.conditions import Key

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
TABLE_NAME = os.environ['TABLE_NAME']

# Tags that map to common research domains
DOMAIN_TAG_MAP = {
    'genomics': ['genomics', 'genetic', 'genome', 'sequencing', 'variant', 'dna', 'rna'],
    'climate': ['climate', 'weather', 'atmospheric', 'temperature', 'precipitation'],
    'satellite': ['satellite', 'earth observation', 'remote sensing', 'imagery', 'radar'],
    'neuroscience': ['neuroscience', 'brain', 'neural', 'eeg', 'fmri'],
    'biodiversity': ['biodiversity', 'ecology', 'species', 'conservation'],
    'astronomy': ['astronomy', 'astrophysics', 'telescope', 'stellar', 'galaxy'],
    'geospatial': ['geospatial', 'geographic', 'mapping', 'gis', 'lidar'],
    'health': ['health', 'medical', 'clinical', 'disease', 'epidemiology'],
    'agriculture': ['agriculture', 'crop', 'soil', 'farming'],
    'oceans': ['oceans', 'marine', 'sea', 'coastal', 'bathymetry'],
    'machine learning': ['machine learning', 'deep learning', 'training data', 'benchmark'],
    'chemistry': ['chemistry', 'molecular', 'compound', 'drug discovery'],
    'economics': ['economics', 'financial', 'census', 'demographic'],
    'transportation': ['transportation', 'traffic', 'vehicle', 'aviation'],
    'energy': ['energy', 'solar', 'wind', 'power', 'grid'],
}


def handler(event: dict, context: Any) -> dict:
    """
    Search the RODA catalog.

    Tool arguments (from AgentCore Gateway, passed directly as event):
    - query: str — natural language search
    - tags: list[str] — filter by tags
    - format: str — filter by detected data format
    - region: str — filter by AWS region
    - max_results: int — default 10, max 50
    - quicksight_compatible: bool — only return CSV/Parquet/JSON datasets
    """
    _tool_name = "unknown"
    try:
        raw = context.client_context.custom["bedrockAgentCoreToolName"]
        _tool_name = raw.split("___")[-1]
    except Exception:
        pass
    logger.info(json.dumps({"tool": _tool_name, "event": event}))

    query = event.get('query', '').strip()
    tags = event.get('tags', [])
    fmt = event.get('format', '')
    region = event.get('region', '')
    try:
        max_results = int(event.get('max_results', 10))
    except (TypeError, ValueError):
        return {'count': 0, 'datasets': [], 'query': '', 'appliedTags': [], 'appliedFormat': '',
                'error': "'max_results' must be an integer"}
    if not (1 <= max_results <= 50):
        return {'count': 0, 'datasets': [], 'query': '', 'appliedTags': [], 'appliedFormat': '',
                'error': "'max_results' must be between 1 and 50"}
    qs_only = event.get('quicksight_compatible', False)

    table = dynamodb.Table(TABLE_NAME)
    results = []

    # Infer domain tags from natural language query
    if query and not tags:
        tags = infer_tags(query)

    # Strategy 1: If we have a single primary tag, use the GSI
    if len(tags) == 1:
        results = query_by_tag(table, tags[0], max_results * 3)
    # Strategy 2: Multiple tags or no tags — scan with filters
    else:
        results = scan_with_filters(table, tags, max_results * 3)

    # Keyword filter on results
    if query:
        keywords = extract_keywords(query)
        results = keyword_rank(results, keywords)

    # Format filter
    if fmt:
        results = [r for r in results if fmt.lower() in r.get('formats', [])]

    # Region filter
    if region:
        results = [
            r for r in results
            if any(s.get('region') == region for s in r.get('s3Resources', []))
        ]

    # QuickSight compatibility filter
    if qs_only:
        qs_formats = {'csv', 'tsv', 'parquet', 'json'}
        results = [
            r for r in results
            if qs_formats.intersection(set(r.get('formats', [])))
        ]

    results = results[:max_results]
    projected = [project_result(r) for r in results]

    return {
        'count': len(projected),
        'datasets': projected,
        'query': query,
        'appliedTags': tags,
        'appliedFormat': fmt,
    }


def query_by_tag(table, tag: str, limit: int) -> list:
    try:
        resp = table.query(
            IndexName='by-primary-tag',
            KeyConditionExpression=Key('primaryTag').eq(tag),
            Limit=limit,
        )
        return resp.get('Items', [])
    except Exception as e:
        logger.warning(f"GSI query failed for tag '{tag}': {e}")
        return []


def scan_with_filters(table, tags: list, limit: int) -> list:
    scan_kwargs = {'Limit': limit}

    if tags:
        filter_parts = []
        expr_values = {}
        for i, tag in enumerate(tags):
            key = f':tag{i}'
            filter_parts.append(f'contains(searchText, {key})')
            expr_values[key] = tag.lower()

        scan_kwargs['FilterExpression'] = ' OR '.join(filter_parts)
        scan_kwargs['ExpressionAttributeValues'] = expr_values

    MAX_SCAN_PAGES = 10

    try:
        resp = table.scan(**scan_kwargs)
        items = resp.get('Items', [])

        page_count = 0
        while 'LastEvaluatedKey' in resp and len(items) < limit and page_count < MAX_SCAN_PAGES:
            page_count += 1
            scan_kwargs['ExclusiveStartKey'] = resp['LastEvaluatedKey']
            resp = table.scan(**scan_kwargs)
            items.extend(resp.get('Items', []))

        if page_count >= MAX_SCAN_PAGES and len(items) < limit:
            logger.warning(f"Scan hit page cap ({MAX_SCAN_PAGES}) with {len(items)} results")

        return items[:limit]
    except Exception as e:
        logger.error(f"Scan failed: {e}")
        return []


def infer_tags(query: str) -> list:
    query_lower = query.lower()
    matched_tags = []
    for tag, keywords in DOMAIN_TAG_MAP.items():
        for kw in keywords:
            if kw in query_lower:
                matched_tags.append(tag)
                break
    return matched_tags


def extract_keywords(query: str) -> list:
    stop_words = {
        'the', 'a', 'an', 'is', 'are', 'was', 'were', 'be', 'been', 'being',
        'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'could',
        'should', 'may', 'might', 'can', 'shall', 'to', 'of', 'in', 'for',
        'on', 'with', 'at', 'by', 'from', 'as', 'into', 'about', 'between',
        'through', 'after', 'before', 'above', 'below', 'and', 'or', 'not',
        'but', 'if', 'then', 'so', 'than', 'too', 'very', 'just', 'only',
        'what', 'which', 'who', 'whom', 'this', 'that', 'these', 'those',
        'i', 'me', 'my', 'we', 'our', 'you', 'your', 'they', 'their',
        'it', 'its', 'any', 'all', 'some', 'find', 'show', 'get', 'list',
        'search', 'look', 'dataset', 'datasets', 'data', 'available',
    }
    words = re.findall(r'[a-z0-9]+', query.lower())
    return [w for w in words if w not in stop_words and len(w) > 2]


def keyword_rank(items: list, keywords: list) -> list:
    if not keywords:
        return items
    scored = []
    for item in items:
        text = item.get('searchText') or ''
        score = sum(1 for kw in keywords if kw in text)
        name = (item.get('name') or '').lower()
        score += sum(3 for kw in keywords if kw in name)
        tags_text = ' '.join(item.get('tags') or [])
        score += sum(2 for kw in keywords if kw in tags_text)
        scored.append((score, item))
    scored.sort(key=lambda x: x[0], reverse=True)
    # Return items with a keyword match; if nothing matched at all, return
    # all results in ranked order so tag/format filters aren't silently discarded.
    filtered = [item for score, item in scored if score > 0]
    return filtered if filtered else [item for _, item in scored]


def project_result(item: dict) -> dict:
    s3_resources = item.get('s3Resources', [])
    primary_bucket = ''
    primary_region = ''
    if s3_resources:
        arn = s3_resources[0].get('arn', '')
        if ':::' in arn:
            primary_bucket = arn.split(':::')[-1].rstrip('/')
        primary_region = s3_resources[0].get('region', '')

    return {
        'slug': item.get('slug', ''),
        'name': item.get('name', ''),
        'description': item.get('description', '')[:400],
        'tags': item.get('tags', []),
        'formats': item.get('formats', []),
        'license': item.get('license', ''),
        'managedBy': item.get('managedBy', ''),
        'updateFrequency': item.get('updateFrequency', ''),
        'primaryBucket': primary_bucket,
        'primaryRegion': primary_region,
        's3ResourceCount': item.get('s3ResourceCount', 0),
        'registryUrl': item.get('registryUrl', ''),
        'quicksightCompatible': bool(
            {'csv', 'tsv', 'parquet', 'json'}.intersection(
                set(item.get('formats', []))
            )
        ),
        'documentation': item.get('documentation', ''),
    }
