# QuickSuite RODA Integration

Brings the [Registry of Open Data on AWS](https://registry.opendata.aws/) into
Amazon QuickSight via the
[quicksuite-model-router](https://github.com/scttfrdmn/quicksuite-model-router).
A researcher types "what climate datasets are available?" and the router's LLM
searches 500+ open datasets, returns matches, and can load selected S3 data
directly into QuickSight as a new data source — no console, no manifest files,
no CLI.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  QuickSight + Model Router                              │
│                                                         │
│  User: "find genomics datasets with variant calls"      │
│        ↓                                                │
│  Router LLM interprets intent, calls RODA tools         │
│        ↓                                                │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────────┐  │
│  │ roda-search  │  │ dataset-info │  │ dataset-load  │  │
│  │   Lambda     │  │   Lambda     │  │   Lambda      │  │
│  └──────┬───────┘  └──────┬───────┘  └──────┬────────┘  │
│         │                 │                 │            │
│  ┌──────▼─────────────────▼─────────────────▼────────┐  │
│  │              DynamoDB: roda-catalog                │  │
│  │  PK: slug  │ name, desc, tags, resources, ...     │  │
│  └──────────────────────▲────────────────────────────┘  │
│                         │                               │
│  ┌──────────────────────┴────────────────────────────┐  │
│  │           catalog-sync Lambda (scheduled)         │  │
│  │  s3://registry.opendata.aws/roda/ndjson/ → DDB    │  │
│  └───────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

## Components

### catalog-sync Lambda
Runs daily (and on SNS notification from RODA). Fetches the NDJSON export
from `s3://registry.opendata.aws/roda/ndjson/`, parses each dataset entry,
and upserts into DynamoDB with searchable fields: name, description, tags,
resource ARNs, regions, data formats, license, and managed-by.

### roda-search Lambda
The LLM tool endpoint. Accepts a natural-language query or structured
filters (tags, format, region) and returns ranked matches. Uses DynamoDB
Scan with filter expressions for tag/keyword matching, with results
re-ranked by the router's LLM for relevance.

### dataset-loader Lambda  
Takes a dataset slug and resource index, generates a QuickSight S3 manifest
file, creates a QuickSight data source + dataset, and returns the dataset ID.
Handles `--no-sign-request` (public) vs requester-pays vs controlled-access
buckets with appropriate IAM.

## Integration with Model Router

Add the RODA tools to your router's `config/routing_config.yaml`:

```yaml
tools:
  roda_search:
    lambda_arn: !GetAtt RodaSearchFunction.Arn
    description: >
      Search the Registry of Open Data on AWS for public datasets.
      Accepts keywords, domain tags (genomics, climate, satellite, etc),
      file format (parquet, csv, zarr, netcdf), or natural language queries.
    parameters:
      query:
        type: string
        description: Natural language search query
      tags:
        type: array
        items: { type: string }
        description: Filter by dataset tags (e.g. genomics, climate, satellite)
      format:
        type: string
        description: Filter by data format (parquet, csv, zarr, netcdf, vcf, bam)
      region:
        type: string
        description: Filter by AWS region
      max_results:
        type: integer
        default: 10

  roda_load_dataset:
    lambda_arn: !GetAtt DatasetLoaderFunction.Arn
    description: >
      Load an open dataset into QuickSight. Creates S3 manifest and
      registers as a QuickSight data source. Only works for datasets
      with tabular data (CSV, Parquet, JSON).
    parameters:
      slug:
        type: string
        description: Dataset slug from search results
      resource_index:
        type: integer
        default: 0
        description: Which S3 resource to use (if dataset has multiple)
      prefix:
        type: string
        description: Optional S3 prefix to narrow to specific partition/subset
      format:
        type: string
        enum: [csv, tsv, parquet, json]
        description: Data format for QuickSight import
```

## Deployment

```bash
# Standalone
cdk deploy RodaIntegrationStack

# With model router (recommended)
cdk deploy QuickSuiteModelRouterStack RodaIntegrationStack

# Seed the catalog immediately after deploy
aws lambda invoke --function-name roda-catalog-sync /dev/null
```

## Cost

| Component | Monthly Cost |
|-----------|-------------|
| DynamoDB (on-demand, ~500 items) | ~$0.25 |
| Lambda (daily sync + searches) | ~$0.10 |
| EventBridge rule | Free |
| **Total infrastructure** | **~$0.35/month** |

S3 data access costs depend on dataset size and requester-pays status.

## Supported Dataset Formats

QuickSight can directly ingest CSV, TSV, JSON, and Parquet from S3.
For other formats (NetCDF, Zarr, VCF, BAM, etc.), the dataset-loader
can optionally route through the ephemeral compute layer (EMR Serverless
or Lambda) from the model router's compute integration to transform data
before loading.

## License

Apache-2.0
