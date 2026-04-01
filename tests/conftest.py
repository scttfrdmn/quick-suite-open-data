"""
Shared fixtures for quick-suite-open-data tests.
"""

import os
import sys
import subprocess
import time
import importlib
import json
import pytest
from unittest.mock import MagicMock, patch

# Fake AWS credentials before any boto3 import
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_SECURITY_TOKEN", "testing")
os.environ.setdefault("AWS_SESSION_TOKEN", "testing")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

# Default Lambda env vars so handler modules can be imported without KeyError
os.environ.setdefault("TABLE_NAME", "qs-open-data-catalog")
os.environ.setdefault("MANIFEST_BUCKET", "qs-manifests-test")
os.environ.setdefault("QUICKSIGHT_ACCOUNT_ID", "123456789012")
os.environ.setdefault("QUICKSIGHT_REGION", "us-east-1")
os.environ.setdefault("SOURCES_CONFIG", "[]")

REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")
# Only add the shared layer to sys.path.  Each test file loads its own
# handler via importlib.util.spec_from_file_location so that multiple
# lambdas/*/handler.py files never collide in sys.modules.
sys.path.insert(0, os.path.join(REPO_ROOT, "lambdas", "common", "python"))

_SUBSTRATE_BIN = os.path.expanduser("~/src/substrate/bin/substrate")


# ---------------------------------------------------------------------------
# Substrate fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def substrate_url():
    """
    Yields the Substrate endpoint URL for integration tests.

    Uses SUBSTRATE_ENDPOINT if set (assumes running externally).
    Otherwise starts ~/src/substrate/bin/substrate.
    Skips integration tests if substrate is unavailable.
    """
    import requests  # noqa: PLC0415

    url = os.environ.get("SUBSTRATE_ENDPOINT", "http://localhost:4566")
    try:
        requests.get(f"{url}/health", timeout=1)
        yield url
        return
    except Exception:
        pass

    if not os.path.exists(_SUBSTRATE_BIN):
        pytest.skip("Substrate binary not found; build ~/src/substrate or set SUBSTRATE_ENDPOINT")
        return

    proc = subprocess.Popen(
        [_SUBSTRATE_BIN, "server"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    for _ in range(40):
        try:
            requests.get(f"{url}/health", timeout=0.5)
            break
        except Exception:
            time.sleep(0.25)
    else:
        proc.terminate()
        pytest.skip("Substrate did not become healthy in time")
        return

    yield url

    proc.terminate()
    proc.wait(timeout=5)


@pytest.fixture
def reset_substrate(substrate_url):
    """Reset all Substrate state before and after the test."""
    import requests  # noqa: PLC0415

    requests.post(f"{substrate_url}/v1/state/reset")
    yield
    requests.post(f"{substrate_url}/v1/state/reset")


# ---------------------------------------------------------------------------
# Catalog data fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def catalog_items():
    return [
        {
            "slug": "noaa-climate",
            "name": "NOAA Global Climate Data",
            "primaryTag": "climate",
            "tags": ["climate", "weather"],
            "description": "Historical climate measurements from NOAA stations worldwide.",
            "searchText": "climate weather temperature precipitation atmospheric",
            "formats": ["csv", "parquet"],
            "regions": ["us-east-1"],
            "s3Resources": [{"arn": "arn:aws:s3:::noaa-climate-data", "region": "us-east-1"}],
            "s3ResourceCount": 1,
            "registryUrl": "https://registry.opendata.aws/noaa-climate/",
            "license": "Open Data",
            "managedBy": "NOAA",
            "updateFrequency": "Daily",
            "documentation": "https://docs.noaa.gov/climate",
        },
        {
            "slug": "ncbi-genome",
            "name": "NCBI 1000 Genomes",
            "primaryTag": "genomics",
            "tags": ["genomics", "bioinformatics"],
            "description": "Whole genome sequencing data from the 1000 Genomes Project.",
            "searchText": "genomics dna rna sequencing variant genome bioinformatics",
            "formats": ["vcf", "bam"],
            "regions": ["us-east-1"],
            "s3Resources": [{"arn": "arn:aws:s3:::ncbi-genome-data", "region": "us-east-1"}],
            "s3ResourceCount": 1,
            "registryUrl": "https://registry.opendata.aws/ncbi-1000-genomes/",
            "license": "Open Data",
            "managedBy": "NCBI",
            "updateFrequency": "Periodic",
            "documentation": "https://www.ncbi.nlm.nih.gov/variation/tools/1000genomes/",
        },
        {
            "slug": "landsat-8",
            "name": "Landsat 8 Satellite Imagery",
            "primaryTag": "satellite",
            "tags": ["satellite", "geospatial"],
            "description": "Multispectral satellite imagery from Landsat 8.",
            "searchText": "satellite imagery landsat geospatial remote sensing earth observation",
            "formats": ["geotiff"],
            "regions": ["us-west-2"],
            "s3Resources": [{"arn": "arn:aws:s3:::landsat-imagery", "region": "us-west-2"}],
            "s3ResourceCount": 1,
            "registryUrl": "https://registry.opendata.aws/landsat-8/",
            "license": "Open Data",
            "managedBy": "USGS",
            "updateFrequency": "Daily",
            "documentation": "https://www.usgs.gov/landsat-missions/landsat-8",
        },
    ]


@pytest.fixture
def s3_sources():
    return [
        {
            "label": "Research Data",
            "bucket": "uni-research-data",
            "prefix": "datasets/",
            "description": "Institutional research datasets",
        },
        {
            "label": "Enrollment",
            "bucket": "uni-enrollment",
            "prefix": "",
            "description": "Student enrollment records",
        },
    ]


@pytest.fixture
def catalog_item_with_s3():
    """Single catalog entry with S3 resources, suitable for dataset-loader tests."""
    return {
        "slug": "noaa-climate",
        "name": "NOAA Global Climate Data",
        "formats": ["csv", "parquet"],
        "s3Resources": [
            {
                "arn": "arn:aws:s3:::noaa-climate-data",
                "region": "us-east-1",
                "requesterPays": False,
            }
        ],
        "registryUrl": "https://registry.opendata.aws/noaa-climate/",
        "documentation": "https://docs.noaa.gov/climate",
    }
