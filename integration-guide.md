# Integration Guide: RODA → QuickSuite Model Router

## Overview

This document describes how to wire the RODA integration into your
existing `quicksuite-model-router` CDK deployment. The integration
adds two LLM tools (search and load) and a background catalog sync,
giving the router's LLM the ability to discover and ingest any of
the 500+ datasets in the Registry of Open Data on AWS.

## Prerequisites

- Deployed `quicksuite-model-router` stack
- QuickSight Enterprise edition (for S3 data source support)
- AWS account with QuickSight configured

## Step 1: Deploy the RODA Stack

```bash
cd quicksuite-roda-integration
npm install
cdk deploy

# Seed the catalog (takes ~30s for the full registry)
npm run seed
```

Note the outputs:
- `RodaIntegrationStack.SearchFunctionArn`
- `RodaIntegrationStack.LoaderFunctionArn`

## Step 2: Register Tools in the Router

Add the RODA tools to your model router. Copy the tool definitions
from `config/roda-tools.yaml` into your router's
`config/routing_config.yaml`, replacing `${RODA_SEARCH_ARN}` and
`${RODA_LOADER_ARN}` with the ARNs from Step 1.

Alternatively, if your router supports cross-stack references:

```yaml
tools:
  roda_search:
    lambda_arn: !ImportValue roda-search-arn
  roda_load_dataset:
    lambda_arn: !ImportValue roda-loader-arn
```

## Step 3: Grant Cross-Invocation Permissions

The model router's orchestrator Lambda needs permission to invoke
the RODA Lambdas. Add to your router stack:

```typescript
rodaSearchFunction.grantInvoke(routerOrchestratorFunction);
rodaLoaderFunction.grantInvoke(routerOrchestratorFunction);
```

Or via IAM policy:

```json
{
  "Effect": "Allow",
  "Action": "lambda:InvokeFunction",
  "Resource": [
    "arn:aws:lambda:*:*:function:roda-search",
    "arn:aws:lambda:*:*:function:roda-dataset-loader"
  ]
}
```

## Step 4: Update System Prompt

Add the RODA system prompt addition from `config/roda-tools.yaml`
to your router's system prompt. This tells the LLM about the RODA
tools and how to present results.

## User Experience

After integration, conversations like this work:

```
User: "What public genomics datasets are available?"

Router: [calls roda_search with query="genomics", quicksight_compatible=true]

LLM: "I found 12 genomics datasets in the Registry of Open Data on AWS.
      Here are the most relevant:

      1. **1000 Genomes Project** — Population-scale whole genome
         sequencing. Parquet format, us-east-1. Free access.

      2. **GIAB Reference Data** — Gold-standard variant calls for
         benchmarking. VCF format (needs transform for QuickSight).

      3. **ClinVar** — Clinical significance of genetic variants.
         CSV format, directly loadable.

      Would you like me to load any of these into QuickSight?"

User: "Load the ClinVar data, just a sample first"

Router: [calls roda_load_dataset with slug="clinvar", sample_only=true]

LLM: "Done — I've loaded a sample of the ClinVar dataset (10 files)
      into QuickSight as 'RODA: ClinVar'. You can now create an
      analysis from it. Want me to load the full dataset?"
```

## Connecting to Ephemeral Compute

For datasets in formats QuickSight can't directly ingest (NetCDF,
Zarr, VCF, BAM, HDF5), the loader returns a `requires_transform`
status. If your router has the ephemeral compute integration
(EMR Serverless / EC2), you can chain these:

1. `roda_search` finds the dataset
2. `roda_load_dataset` reports `requires_transform`
3. Router routes to compute layer to convert to Parquet
4. Converted data loads into QuickSight

This is the full pipeline: open data → transform → visualize,
all from a natural language request.

## Cost Considerations

The RODA integration itself costs under $1/month. The real cost
is data access:

- **No-sign-request buckets**: Free from same region, standard
  egress charges cross-region
- **Requester-pays buckets**: You pay for GET requests and egress
- **Large datasets**: SPICE import has per-GB costs in QuickSight

The `sample_only` flag on the loader is your cost control — always
preview before full load on large datasets.

## Architecture Notes

The catalog sync uses the official RODA NDJSON export at
`s3://registry.opendata.aws/roda/ndjson/` rather than parsing
YAML from GitHub. This is the machine-readable form maintained by
the RODA team, with an SNS topic (`roda-object_created`) for
real-time update notifications.

The DynamoDB table uses on-demand billing and a GSI on `primaryTag`
for fast domain-scoped queries. The search Lambda falls back to
scan with keyword ranking for cross-domain or natural language
queries. For production use with heavy search traffic, consider
adding OpenSearch Serverless as an index.
