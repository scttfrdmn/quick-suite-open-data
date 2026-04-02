"""
Shared data utilities for quick-suite-open-data Lambdas.

Used by: catalog-sync, s3-preview, s3-load
"""

import csv
import io
import json
import logging
import re

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Format detection
# ---------------------------------------------------------------------------

FORMAT_PATTERNS = {
    'parquet': r'\bparquet\b',
    'csv': r'\bcsv\b',
    'tsv': r'\btsv\b',
    'json': r'\bjson\b',
    'ndjson': r'\bndjson\b',
    'zarr': r'\bzarr\b',
    'netcdf': r'\bnetcdf\b|\.nc\b',
    'geotiff': r'\bgeotiff\b|\.tif\b',
    'cog': r'\bcog\b|cloud.optimized.geotiff',
    'vcf': r'\bvcf\b',
    'bam': r'\bbam\b',
    'fastq': r'\bfastq\b',
    'hdf5': r'\bhdf5?\b|\.h5\b',
    'grib': r'\bgrib\b',
    'shapefile': r'\bshapefile\b|\.shp\b',
}


def detect_formats(resources: list, description: str) -> list[str]:
    """
    Detect likely data formats from resource descriptions and metadata.
    Extracted from catalog-sync for reuse across Lambdas.
    """
    formats: set[str] = set()
    text = description.lower()

    for r in resources:
        rdesc = (r.get('Description', '') or '').lower()
        text += ' ' + rdesc
        for explore in (r.get('Explore', []) or []):
            if isinstance(explore, str):
                text += ' ' + explore.lower()

    for fmt, pattern in FORMAT_PATTERNS.items():
        if re.search(pattern, text):
            formats.add(fmt)

    return sorted(formats)


def detect_format_from_key(key: str) -> str:
    """Infer format from an S3 object key extension."""
    key_lower = key.lower()
    if key_lower.endswith('.parquet') or key_lower.endswith('.snappy.parquet') or key_lower.endswith('.parq'):
        return 'parquet'
    if key_lower.endswith('.csv') or key_lower.endswith('.csv.gz'):
        return 'csv'
    if key_lower.endswith('.tsv') or key_lower.endswith('.tsv.gz') or key_lower.endswith('.tab') or key_lower.endswith('.tab.gz'):
        return 'tsv'
    if key_lower.endswith('.json') or key_lower.endswith('.json.gz'):
        return 'json'
    if key_lower.endswith('.jsonl') or key_lower.endswith('.ndjson'):
        return 'ndjson'
    return ''


# ---------------------------------------------------------------------------
# Schema inference
# ---------------------------------------------------------------------------

def infer_schema_from_csv(content: bytes, max_rows: int = 5) -> dict:
    """
    Infer column names and sample values from CSV bytes.
    Returns {'columns': [...], 'sample_rows': [...], 'row_count': n}.
    """
    try:
        text = content.decode('utf-8', errors='replace')
        reader = csv.DictReader(io.StringIO(text))
        columns = reader.fieldnames or []
        sample_rows = []
        for i, row in enumerate(reader):
            if i >= max_rows:
                break
            sample_rows.append(dict(row))
        return {
            'columns': list(columns),
            'sample_rows': sample_rows,
            'row_count': len(sample_rows),
            'format': 'csv',
        }
    except Exception as e:
        logger.warning(f"CSV schema inference failed: {e}")
        return {'columns': [], 'sample_rows': [], 'row_count': 0, 'format': 'csv', 'error': str(e)}


def infer_schema_from_json(content: bytes, max_rows: int = 5) -> dict:
    """
    Infer schema from JSON or NDJSON bytes.
    Handles both a JSON array and newline-delimited JSON objects.
    """
    try:
        text = content.decode('utf-8', errors='replace').strip()
        rows = []

        # Try NDJSON first
        if text.startswith('{'):
            for line in text.split('\n')[:max_rows]:
                line = line.strip()
                if line:
                    try:
                        rows.append(json.loads(line))
                    except json.JSONDecodeError as exc:
                        logger.warning(f"Skipping malformed JSON line in schema inference: {exc}")
                        continue
        else:
            data = json.loads(text)
            if isinstance(data, list):
                rows = data[:max_rows]
            elif isinstance(data, dict):
                rows = [data]

        columns = list(rows[0].keys()) if rows else []
        return {
            'columns': columns,
            'sample_rows': rows[:max_rows],
            'row_count': len(rows),
            'format': 'json',
        }
    except Exception as e:
        logger.warning(f"JSON schema inference failed: {e}")
        return {'columns': [], 'sample_rows': [], 'row_count': 0, 'format': 'json', 'error': str(e)}


def infer_schema_from_parquet(content: bytes) -> dict:
    """Infer schema and sample rows from Parquet bytes. Requires pyarrow layer."""
    try:
        import io

        import pyarrow.parquet as pq
        table = pq.read_table(io.BytesIO(content))
        schema = table.schema
        columns = [field.name for field in schema]
        n = min(5, table.num_rows)
        sample_dict = table.slice(0, n).to_pydict()
        rows = [{col: sample_dict[col][i] for col in columns} for i in range(n)]
        return {
            'columns': columns,
            'sample_rows': rows,
            'row_count': table.num_rows,
            'format': 'parquet',
            'parquet_schema': str(schema),
        }
    except ImportError:
        return {'columns': [], 'sample_rows': [], 'row_count': 0, 'format': 'parquet',
                'note': 'Schema inference for parquet requires the pyarrow Lambda layer.'}
    except Exception as e:
        logger.warning(f"Parquet schema inference failed: {e}")
        return {'columns': [], 'sample_rows': [], 'row_count': 0, 'format': 'parquet',
                'error': str(e)}


def infer_schema_from_bytes(content: bytes, fmt: str) -> dict:
    """Route schema inference to the right handler based on format."""
    if fmt in ('csv', 'tsv'):
        return infer_schema_from_csv(content)
    if fmt in ('json', 'ndjson'):
        return infer_schema_from_json(content)
    if fmt == 'parquet':
        return infer_schema_from_parquet(content)
    return {'columns': [], 'sample_rows': [], 'row_count': 0, 'format': fmt,
            'note': f'Schema inference for {fmt} is not supported.'}
