"""
E2E conftest for quick-suite-data.

Runs against a deployed QuickSuiteOpenData CloudFormation stack using real AWS.
All tests skip automatically when the stack is not deployed or credentials are absent.

Required environment:
  AWS_PROFILE=aws           (or other standard AWS credential env vars)

Optional environment:
  QS_E2E_STACK_NAME         CloudFormation stack name (default: QuickSuiteOpenData)
  QS_E2E_REGION             AWS region (default: us-east-1)
  QS_E2E_ALLOW_WRITES       Set to "true" to enable QuickSight write tests (incurs cost)

Run:
  AWS_PROFILE=aws pytest tests/e2e/ -v -m e2e
  AWS_PROFILE=aws QS_E2E_ALLOW_WRITES=true pytest tests/e2e/ -v -m e2e
"""

import json
import os
import time
from typing import Any

import boto3
import pytest

STACK_NAME = os.environ.get("QS_E2E_STACK_NAME", "QuickSuiteOpenData")
REGION = os.environ.get("QS_E2E_REGION", "us-east-1")
# Honor AWS_PROFILE explicitly — the parent tests/conftest.py sets fake credential
# env vars via os.environ.setdefault, which would shadow the real profile.
_AWS_PROFILE = os.environ.get("AWS_PROFILE")


def _session() -> "boto3.Session":
    """Return a boto3 Session using the explicit AWS_PROFILE if set."""
    import boto3 as _boto3
    if _AWS_PROFILE:
        return _boto3.Session(profile_name=_AWS_PROFILE, region_name=REGION)
    return _boto3.Session(region_name=REGION)

# Slug prefix used for all seeded test items — enables targeted cleanup
E2E_SLUG_PREFIX = "e2e-test-"

# ---------------------------------------------------------------------------
# Seed data for roda-search and catalog-quality-check E2E tests
# ---------------------------------------------------------------------------

_NOW = int(time.time())

E2E_SEED_ITEMS = [
    {
        "slug": "e2e-test-climate",
        "name": "E2E Climate Dataset",
        "primaryTag": "climate",
        "tags": ["climate", "weather"],
        "searchText": "e2e climate weather temperature atmospheric synth",
        "description": "Synthetic E2E test dataset for climate research",
        "formats": ["csv", "parquet"],
        "regions": ["us-east-1"],
        "s3Resources": [{"arn": "arn:aws:s3:::noaa-ghcn-pds", "region": "us-east-1",
                         "requesterPays": False}],
        "s3ResourceCount": 1,
        "registryUrl": "https://registry.opendata.aws/noaa-ghcn/",
        "license": "Open Data",
        "managedBy": "NOAA",
        "updateFrequency": "Daily",
        "documentation": "https://www.ncei.noaa.gov/products/land-based-station/global-historical-climatology-network-daily",
        "last_updated": _NOW - 86400,  # yesterday — not stale
    },
    {
        "slug": "e2e-test-genomics",
        "name": "E2E Genomics Dataset",
        "primaryTag": "genomics",
        "tags": ["genomics", "sequencing"],
        "searchText": "e2e genomics sequencing variant dna rna synth",
        "description": "Synthetic E2E test dataset for genomics research",
        "formats": ["vcf", "bam"],
        "regions": ["us-east-1"],
        "s3Resources": [{"arn": "arn:aws:s3:::1000genomes", "region": "us-east-1",
                         "requesterPays": False}],
        "s3ResourceCount": 1,
        "registryUrl": "https://registry.opendata.aws/1000-genomes/",
        "license": "Open Data",
        "managedBy": "NIH",
        "updateFrequency": "Monthly",
        "last_updated": _NOW - 30 * 86400,
    },
    {
        "slug": "e2e-test-stale",
        "name": "E2E Stale Dataset",
        "primaryTag": "climate",
        "tags": ["climate"],
        "searchText": "e2e stale old dataset synth",
        "description": "Synthetic stale item for quality-check E2E tests",
        "formats": ["csv"],
        # No last_updated — will be flagged as stale by quality-check
    },
]


# ---------------------------------------------------------------------------
# Invoke helper
# ---------------------------------------------------------------------------

def invoke(lam_client: Any, arn: str, payload: dict) -> dict:
    """Invoke a Lambda function and return the parsed response dict."""
    response = lam_client.invoke(
        FunctionName=arn,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload).encode(),
    )
    raw = response["Payload"].read()
    if response.get("FunctionError"):
        pytest.fail(f"Lambda function error invoking {arn}: {raw.decode()}")
    return json.loads(raw)


# ---------------------------------------------------------------------------
# Session-scoped AWS client fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def lam():
    from botocore.config import Config
    # Lambda functions can run up to 5 minutes; raise the client read timeout
    # above the default 60s so we don't time out waiting for slow cold starts
    # or S3-listing operations inside the handler.
    return _session().client("lambda", config=Config(read_timeout=300, connect_timeout=10))


@pytest.fixture(scope="session")
def ddb_resource():
    return _session().resource("dynamodb")


@pytest.fixture(scope="session")
def s3_client():
    return _session().client("s3")


@pytest.fixture(scope="session")
def cw_client():
    return _session().client("cloudwatch")


# ---------------------------------------------------------------------------
# CloudFormation outputs
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def cfn_outputs() -> dict[str, str]:
    """
    Fetch CloudFormation outputs for the deployed QuickSuiteOpenData stack.
    All E2E tests skip if the stack is not deployed or credentials are absent.
    Set AWS_PROFILE=aws before running.
    """
    cfn = _session().client("cloudformation")
    try:
        resp = cfn.describe_stacks(StackName=STACK_NAME)
    except Exception as exc:
        pytest.skip(
            f"Stack '{STACK_NAME}' not found or no AWS credentials "
            f"(set AWS_PROFILE=aws and QS_E2E_STACK_NAME if needed): {exc}"
        )
    stacks = resp.get("Stacks", [])
    if not stacks:
        pytest.skip(f"Stack '{STACK_NAME}' returned no data")
    raw = stacks[0].get("Outputs", [])
    return {o["OutputKey"]: o["OutputValue"] for o in raw}


@pytest.fixture(scope="session")
def tool_arns(cfn_outputs) -> dict[str, str]:
    raw = cfn_outputs.get("ToolArns", "{}")
    return json.loads(raw)


@pytest.fixture(scope="session")
def catalog_table_name(cfn_outputs) -> str:
    return cfn_outputs["CatalogTableName"]


@pytest.fixture(scope="session")
def manifest_bucket_name(cfn_outputs) -> str:
    return cfn_outputs["ManifestBucketName"]


@pytest.fixture(scope="session")
def catalog_sync_arn(cfn_outputs) -> str:
    return cfn_outputs["CatalogSyncArn"]


@pytest.fixture(scope="session")
def catalog_quality_check_arn(cfn_outputs) -> str:
    return cfn_outputs["CatalogQualityCheckArn"]


@pytest.fixture(scope="session")
def roda_sync_result(lam, catalog_sync_arn) -> dict:
    """Invoke catalog-sync exactly once per session; reused across all tests that need it."""
    return invoke(lam, catalog_sync_arn, {})


@pytest.fixture(scope="session")
def quality_check_result(lam, catalog_quality_check_arn, seeded_catalog) -> dict:
    """
    Invoke catalog-quality-check exactly once per session after seeding.
    Depends on seeded_catalog to ensure e2e-test-stale is present before the scan.
    """
    return invoke(lam, catalog_quality_check_arn, {})


# ---------------------------------------------------------------------------
# Catalog seeding
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def seeded_catalog(catalog_table_name, ddb_resource):
    """
    Write synthetic E2E test items into the deployed DynamoDB catalog table.
    Yields the list of seeded items. Deletes them (best-effort) on session teardown.
    """
    table = ddb_resource.Table(catalog_table_name)
    for item in E2E_SEED_ITEMS:
        table.put_item(Item=item)
    yield E2E_SEED_ITEMS
    for item in E2E_SEED_ITEMS:
        try:
            table.delete_item(Key={"slug": item["slug"]})
        except Exception:
            pass


# ---------------------------------------------------------------------------
# SOURCES_CONFIG introspection for s3-browse / s3-preview tests
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def sources_config(lam, tool_arns) -> list[dict]:
    """Read SOURCES_CONFIG from the deployed s3-browse Lambda env."""
    resp = lam.get_function_configuration(FunctionName=tool_arns["s3_browse"])
    raw = resp.get("Environment", {}).get("Variables", {}).get("SOURCES_CONFIG", "[]")
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return []


@pytest.fixture(scope="session")
def first_source(lam, tool_arns, sources_config) -> dict:
    """
    First configured source that is actually accessible (browse returns no error).
    Skips if no sources are configured or none are accessible.
    This prevents s3-browse tests from failing on placeholder bucket names.
    """
    if not sources_config:
        pytest.skip(
            "No sources in deployed SOURCES_CONFIG — add entries to config/sources.yaml and redeploy"
        )
    for source in sources_config:
        result = invoke(lam, tool_arns["s3_browse"], {"source": source["label"]})
        if "error" not in result:
            return source
    pytest.skip(
        "No accessible sources found in deployed SOURCES_CONFIG — "
        "configure real S3 buckets (e.g. RODA buckets) in config/sources.yaml and redeploy"
    )


# ---------------------------------------------------------------------------
# QuickSight write opt-in
# ---------------------------------------------------------------------------

def _qs_writes_allowed() -> bool:
    return os.environ.get("QS_E2E_ALLOW_WRITES", "").lower() in ("1", "true", "yes")


@pytest.fixture
def require_qs_writes():
    """Skip unless QS_E2E_ALLOW_WRITES=true. Use as a fixture parameter."""
    if not _qs_writes_allowed():
        pytest.skip(
            "QuickSight write tests disabled — set QS_E2E_ALLOW_WRITES=true to enable "
            "(creates real QS data sources/datasets, incurs cost)"
        )


# ---------------------------------------------------------------------------
# Cleanup helpers
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def claws_lookup_table_name(lam, tool_arns) -> "str | None":
    """CLAWS_LOOKUP_TABLE env var from the deployed roda-load Lambda (may be absent)."""
    resp = lam.get_function_configuration(FunctionName=tool_arns["roda_load"])
    return resp.get("Environment", {}).get("Variables", {}).get("CLAWS_LOOKUP_TABLE") or None


@pytest.fixture
def claws_lookup_cleanup(claws_lookup_table_name, ddb_resource):
    """Track claws_source_ids written during a test and delete them on teardown."""
    written_ids: list[str] = []

    class _Tracker:
        def register(self, source_id: str) -> None:
            written_ids.append(source_id)

    tracker = _Tracker()
    yield tracker
    if not claws_lookup_table_name:
        return
    table = ddb_resource.Table(claws_lookup_table_name)
    for sid in written_ids:
        try:
            table.delete_item(Key={"source_id": sid})
        except Exception:
            pass


@pytest.fixture
def manifest_cleanup(s3_client, manifest_bucket_name):
    """Track manifest S3 keys written during a test and delete them on teardown."""
    written_keys: list[str] = []

    class _Tracker:
        def register(self, key: str) -> None:
            written_keys.append(key)

    tracker = _Tracker()
    yield tracker
    for key in written_keys:
        try:
            s3_client.delete_object(Bucket=manifest_bucket_name, Key=key)
        except Exception:
            pass


@pytest.fixture
def qs_cleanup(cfn_outputs):
    """Track QuickSight resource IDs created during a test and delete them on teardown."""
    qs = _session().client("quicksight")
    sts = _session().client("sts")
    account_id = sts.get_caller_identity()["Account"]
    created_datasets: list[str] = []
    created_sources: list[str] = []
    yield created_datasets, created_sources
    for ds_id in created_datasets:
        try:
            qs.delete_data_set(AwsAccountId=account_id, DataSetId=ds_id)
        except Exception:
            pass
    for src_id in created_sources:
        try:
            qs.delete_data_source(AwsAccountId=account_id, DataSourceId=src_id)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Session-scoped write results (invoke each write Lambda once per session)
# ---------------------------------------------------------------------------

def _register_write_result_cleanup(request, result, manifest_bucket_name,
                                    s3_client, claws_lookup_table_name, ddb_resource):
    """Register cleanup finalizers for a write Lambda result dict."""
    qs = _session().client("quicksight")
    sts = _session().client("sts")
    account_id = sts.get_caller_identity()["Account"]

    dataset_id = result.get("datasetId")
    source_id = result.get("quicksightResult", {}).get("dataSourceId")
    claws_id = result.get("claws_source_id")
    manifest_uri = result.get("manifestUri", "")
    manifest_key = manifest_uri.split(f"s3://{manifest_bucket_name}/", 1)[-1] if manifest_uri else None

    def _cleanup():
        if dataset_id:
            try:
                qs.delete_data_set(AwsAccountId=account_id, DataSetId=dataset_id)
            except Exception:
                pass
        if source_id:
            try:
                qs.delete_data_source(AwsAccountId=account_id, DataSourceId=source_id)
            except Exception:
                pass
        if claws_id and claws_lookup_table_name:
            try:
                ddb_resource.Table(claws_lookup_table_name).delete_item(
                    Key={"source_id": claws_id}
                )
            except Exception:
                pass
        if manifest_key:
            try:
                s3_client.delete_object(Bucket=manifest_bucket_name, Key=manifest_key)
            except Exception:
                pass

    request.addfinalizer(_cleanup)


@pytest.fixture(scope="session")
def roda_load_write_result(request, lam, tool_arns, seeded_catalog,
                            manifest_bucket_name, s3_client,
                            claws_lookup_table_name, ddb_resource) -> "dict | None":
    """
    Invoke roda_load once per session with full-load params.
    Returns None when QS_E2E_ALLOW_WRITES is not set (tests skip themselves).
    Registers cleanup of any QS/S3/DDB resources on session teardown.
    """
    if not _qs_writes_allowed():
        return None
    result = invoke(lam, tool_arns["roda_load"], {"slug": "e2e-test-climate", "format": "csv"})
    _register_write_result_cleanup(request, result, manifest_bucket_name,
                                   s3_client, claws_lookup_table_name, ddb_resource)
    return result


@pytest.fixture(scope="session")
def s3_load_write_result(request, lam, tool_arns, first_source,
                          manifest_bucket_name, s3_client,
                          claws_lookup_table_name, ddb_resource) -> "dict | None":
    """
    Invoke s3_load once per session with full-load params.
    Returns None when QS_E2E_ALLOW_WRITES is not set (tests skip themselves).
    Registers cleanup of any QS/S3/DDB resources on session teardown.
    """
    if not _qs_writes_allowed():
        return None
    result = invoke(lam, tool_arns["s3_load"],
                    {"source": first_source["label"], "format": "csv"})
    _register_write_result_cleanup(request, result, manifest_bucket_name,
                                   s3_client, claws_lookup_table_name, ddb_resource)
    return result
