"""
register_memory_source — Register a clAWS memory NDJSON file as a QuickSight dataset.

Invoked via Lambda-to-Lambda call from the clAWS remember tool.
NOT an API Gateway endpoint.

Input (event dict):
    user_arn_hash   str  SHA-256 prefix of user ARN (S3 path component)
    memory_s3_uri   str  S3 URI of the NDJSON findings file
    dataset_label   str  Human-readable QuickSight dataset identifier
    aws_account_id  str  AWS account ID for QuickSight API calls

Output dict:
    dataset_id      str  QuickSight DataSet ID
    source_id       str  Same as dataset_id (kept for API compatibility)
    data_source_id  str  QuickSight DataSource ID
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import UTC, datetime
from typing import Any

import boto3

logger = logging.getLogger(__name__)

REGISTRY_TABLE = os.environ.get("REGISTRY_TABLE", "qs-claws-memory-registry")
MANIFEST_BUCKET = os.environ.get("MANIFEST_BUCKET", "")
QUICKSIGHT_ACCOUNT_ID = os.environ.get("QUICKSIGHT_ACCOUNT_ID", "")
QUICKSIGHT_REGION = os.environ.get("QUICKSIGHT_REGION", "us-east-1")

_qs = None
_s3 = None
_dynamodb = None


def _quicksight():
    global _qs
    if _qs is None:
        _qs = boto3.client("quicksight", region_name=QUICKSIGHT_REGION)
    return _qs


def _s3_client():
    global _s3
    if _s3 is None:
        _s3 = boto3.client("s3")
    return _s3


def _ddb():
    global _dynamodb
    if _dynamodb is None:
        _dynamodb = boto3.resource("dynamodb")
    return _dynamodb


def handler(event: dict, context: Any) -> dict:
    user_arn_hash = str(event.get("user_arn_hash", "")).strip()
    memory_s3_uri = str(event.get("memory_s3_uri", "")).strip()
    dataset_label = str(event.get("dataset_label", "claws-memory")).strip()
    aws_account_id = str(event.get("aws_account_id", QUICKSIGHT_ACCOUNT_ID)).strip()

    if not user_arn_hash:
        return {"error": "user_arn_hash is required", "statusCode": 400}
    if not memory_s3_uri or not memory_s3_uri.startswith("s3://"):
        return {"error": "memory_s3_uri must be a valid s3:// URI", "statusCode": 400}
    if not aws_account_id:
        return {"error": "aws_account_id is required", "statusCode": 400}

    # Idempotency: check registry before doing any work
    try:
        table = _ddb().Table(REGISTRY_TABLE)
        resp = table.get_item(
            Key={"user_arn_hash": user_arn_hash, "dataset_type": "findings"}
        )
        if "Item" in resp:
            item = resp["Item"]
            logger.info(f"register_memory_source: cache hit for {user_arn_hash}")
            return {
                "dataset_id": item["dataset_id"],
                "source_id": item["dataset_id"],
                "data_source_id": item["data_source_id"],
            }
    except Exception as exc:
        logger.warning(f"register_memory_source: registry check failed: {exc}")

    # Parse S3 URI
    parts = memory_s3_uri[5:].split("/", 1)
    s3_bucket = parts[0]
    s3_key = parts[1] if len(parts) > 1 else ""

    # Unique IDs for QuickSight resources
    uid = str(uuid.uuid4())[:8]
    data_source_id = f"claws-memory-src-{user_arn_hash[:12]}-{uid}"
    dataset_id = f"claws-memory-ds-{user_arn_hash[:12]}-{uid}"
    now = datetime.now(UTC).isoformat()

    # Write S3 manifest (QuickSight requires manifest for NDJSON)
    manifest = {
        "fileLocations": [{"URIs": [memory_s3_uri]}],
        "globalUploadSettings": {
            "format": "JSON",
            "containsHeader": False,
        },
    }
    manifest_key = f"manifests/memory/{user_arn_hash}/findings-manifest.json"
    try:
        if MANIFEST_BUCKET:
            _s3_client().put_object(
                Bucket=MANIFEST_BUCKET,
                Key=manifest_key,
                Body=json.dumps(manifest).encode(),
                ContentType="application/json",
            )
            manifest_uri = f"s3://{MANIFEST_BUCKET}/{manifest_key}"
        else:
            # Fallback: write manifest next to the findings file
            manifest_bucket = s3_bucket
            manifest_key_fallback = s3_key.rsplit("/", 1)[0] + "/findings-manifest.json"
            _s3_client().put_object(
                Bucket=manifest_bucket,
                Key=manifest_key_fallback,
                Body=json.dumps(manifest).encode(),
                ContentType="application/json",
            )
            manifest_uri = f"s3://{manifest_bucket}/{manifest_key_fallback}"
    except Exception as exc:
        logger.error(f"register_memory_source: manifest write failed: {exc}")
        return {"error": f"Failed to write S3 manifest: {exc}", "statusCode": 500}

    # Create QuickSight DataSource
    try:
        _quicksight().create_data_source(
            AwsAccountId=aws_account_id,
            DataSourceId=data_source_id,
            Name=f"claws-memory-{user_arn_hash[:12]}",
            Type="S3",
            DataSourceParameters={
                "S3Parameters": {"ManifestFileLocation": {
                    "Bucket": manifest_uri.split("/")[2],
                    "Key": "/".join(manifest_uri.split("/")[3:]),
                }}
            },
            Permissions=[{
                "Principal": (
                    f"arn:aws:quicksight:{QUICKSIGHT_REGION}:{aws_account_id}"
                    f":user/default/{os.environ.get('QUICKSIGHT_USER', 'admin')}"
                ),
                "Actions": [
                    "quicksight:DescribeDataSource",
                    "quicksight:DescribeDataSourcePermissions",
                    "quicksight:PassDataSource",
                ],
            }],
        )
    except Exception as exc:
        logger.error(f"register_memory_source: create_data_source failed: {exc}")
        return {"error": f"QuickSight DataSource creation failed: {exc}", "statusCode": 500}

    # Create QuickSight Dataset
    try:
        _quicksight().create_data_set(
            AwsAccountId=aws_account_id,
            DataSetId=dataset_id,
            Name=f"{dataset_label}-{user_arn_hash[:12]}",
            PhysicalTableMap={
                "findings": {
                    "S3Source": {
                        "DataSourceArn": (
                            f"arn:aws:quicksight:{QUICKSIGHT_REGION}"
                            f":{aws_account_id}:datasource/{data_source_id}"
                        ),
                        "UploadSettings": {"Format": "JSON", "ContainsHeader": False},
                        "InputColumns": [
                            {"Name": "memory_id", "Type": "STRING"},
                            {"Name": "subject", "Type": "STRING"},
                            {"Name": "fact", "Type": "STRING"},
                            {"Name": "confidence", "Type": "DECIMAL"},
                            {"Name": "tags", "Type": "STRING"},
                            {"Name": "severity", "Type": "STRING"},
                            {"Name": "recorded_at", "Type": "DATETIME"},
                            {"Name": "expires_at", "Type": "DATETIME"},
                            {"Name": "source_plan_id", "Type": "STRING"},
                        ],
                    }
                }
            },
            ImportMode="SPICE",
            Permissions=[{
                "Principal": (
                    f"arn:aws:quicksight:{QUICKSIGHT_REGION}:{aws_account_id}"
                    f":user/default/{os.environ.get('QUICKSIGHT_USER', 'admin')}"
                ),
                "Actions": [
                    "quicksight:DescribeDataSet",
                    "quicksight:DescribeDataSetPermissions",
                    "quicksight:PassDataSet",
                    "quicksight:DescribeIngestion",
                    "quicksight:ListIngestions",
                ],
            }],
        )
    except Exception as exc:
        logger.error(f"register_memory_source: create_data_set failed: {exc}")
        return {"error": f"QuickSight DataSet creation failed: {exc}", "statusCode": 500}

    # Trigger SPICE ingestion
    try:
        ingestion_id = f"init-{uid}"
        _quicksight().create_ingestion(
            DataSetId=dataset_id,
            IngestionId=ingestion_id,
            AwsAccountId=aws_account_id,
        )
    except Exception as exc:
        logger.warning(f"register_memory_source: SPICE ingestion trigger failed (non-fatal): {exc}")

    # Write registry entry
    try:
        table = _ddb().Table(REGISTRY_TABLE)
        table.put_item(Item={
            "user_arn_hash": user_arn_hash,
            "dataset_type": "findings",
            "dataset_id": dataset_id,
            "source_id": dataset_id,
            "data_source_id": data_source_id,
            "memory_s3_uri": memory_s3_uri,
            "dataset_label": dataset_label,
            "aws_account_id": aws_account_id,
            "registered_at": now,
        })
    except Exception as exc:
        logger.error(f"register_memory_source: registry write failed: {exc}")
        # Non-fatal — QuickSight resources created; next call will create again
        # but succeed idempotently
        logger.warning("Registry write failed; resources created but idempotency may be broken")

    return {
        "dataset_id": dataset_id,
        "source_id": dataset_id,
        "data_source_id": data_source_id,
    }
