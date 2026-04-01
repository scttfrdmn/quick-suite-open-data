# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-04-01

### Added
- Parquet schema inference in `s3_preview` via Docker-built pyarrow Lambda layer (`lambdas/pyarrow-layer/Dockerfile`); attached to `s3-preview` Lambda only — other Lambdas are unaffected
- Post-deploy helper script (`scripts/post-deploy.sh`) — retrieves all tool Lambda ARNs from CloudFormation and prints AgentCore Gateway registration commands for all five tools
- AgentCore Gateway registration guide (`docs/agentcore-registration.md`) — console and CLI registration steps, invoke permission setup, and verification
- Institutional S3 sources configuration guide (`docs/sources-config.md`) — format reference, IAM security model, examples for university/lab/government configurations, and redeploy workflow

## [0.1.0] - 2026-04-01

### Added
- Daily catalog sync from RODA NDJSON into DynamoDB (`catalog-sync` Lambda), with optional real-time SNS trigger
- Dataset search across 500+ public AWS open datasets (`roda_search` AgentCore tool)
- One-click dataset loading into Quick Sight via S3 manifest (`roda_load` AgentCore tool)
- Institutional S3 bucket browsing with prefix navigation (`s3_browse` AgentCore tool)
- Schema inference and row sampling from S3 files (`s3_preview` AgentCore tool) — supports CSV, TSV, Parquet, JSON, NDJSON
- S3 path registration as Quick Sight data source (`s3_load` AgentCore tool)
- Shared `data_utils` Lambda layer for format detection and schema inference
- CDK stack with AgentCore Gateway Lambda target wiring and IAM policies scoped to configured sources

[unreleased]: https://github.com/scttfrdmn/quick-suite-open-data/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/scttfrdmn/quick-suite-open-data/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/scttfrdmn/quick-suite-open-data/releases/tag/v0.1.0
