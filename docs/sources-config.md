# Institutional S3 Sources Configuration

This guide explains how to configure `config/sources.yaml` to grant Quick Suite
access to your institution's private S3 data alongside public RODA datasets.

---

## What This Controls

The `sources.yaml` file is the **complete allow-list** of S3 buckets that the
`s3_browse`, `s3_preview`, and `s3_load` tools can access. The CDK stack
generates IAM policies from this list at deploy time — no bucket outside this
list can ever be reached, regardless of what a user requests.

---

## Configuration Format

```yaml
institutional_sources:
  - label: "Human-readable name shown in Quick Suite"
    bucket: your-s3-bucket-name        # required; bucket name only, no s3:// prefix
    prefix: optional/path/prefix/       # optional; restrict access to a path
    description: "Shown to users when browsing"
```

| Field | Required | Description |
|-------|----------|-------------|
| `label` | yes | Display name for the tool. Shown in `s3_browse` listings. |
| `bucket` | yes | S3 bucket name (no `s3://` prefix, no trailing slash). |
| `prefix` | no | Restricts access to objects under this path. Include trailing `/`. Empty string = full bucket. |
| `description` | yes | Plain-text description shown to Quick Suite users. |

---

## Setup Steps

**1. Copy the example file:**
```bash
cp config/sources.example.yaml config/sources.yaml
```

**2. Edit `config/sources.yaml`** with your institution's buckets:
```yaml
institutional_sources:
  - label: "Research Data Commons"
    bucket: my-university-research-data
    prefix: shared/
    description: "Cross-department research datasets managed by Research Computing"

  - label: "Institutional Research"
    bucket: my-ir-office-data
    prefix: ""
    description: "Enrollment, retention, and graduation data from IR office"
```

**3. Deploy:**
```bash
cdk deploy
```

The CDK stack reads `sources.yaml` at synth time and generates the IAM policy.
Every `cdk deploy` regenerates the policy — add or remove entries freely.

---

## IAM Security Model

The CDK generates one `s3:GetObject` + `s3:ListBucket` policy statement per
source entry. If you specify a prefix, the policy is restricted to
`arn:aws:s3:::bucket/prefix*`. Without a prefix, it covers `arn:aws:s3:::bucket/*`.

The three S3 tool Lambdas (`s3_browse`, `s3_preview`, `s3_load`) share a
single IAM role with exactly these permissions. They cannot access any bucket
not listed in `sources.yaml`.

---

## Example Configurations

### University with departmental buckets

```yaml
institutional_sources:
  - label: "Research Computing Shared Storage"
    bucket: hpc-research-data-prod
    prefix: shared/
    description: "HPC job outputs and shared datasets from Research Computing"

  - label: "Institutional Research"
    bucket: ir-analytics-prod
    prefix: ""
    description: "Enrollment and outcomes data from Institutional Research"

  - label: "Finance Analytics"
    bucket: finance-data-warehouse
    prefix: exports/quicksuite/
    description: "Budget and expenditure exports approved for Quick Suite access"
```

### National laboratory with project-scoped access

```yaml
institutional_sources:
  - label: "Climate Modeling Output (Project A)"
    bucket: lab-compute-outputs
    prefix: climate-model-a/results/
    description: "Climate Model A simulation outputs, post-2023"

  - label: "Genomics Pipeline Results"
    bucket: lab-compute-outputs
    prefix: genomics/pipeline-v3/
    description: "Genomics pipeline v3 processed results"
```

Note: you can reference the **same bucket twice** with different prefixes to
grant access to distinct project paths without exposing the full bucket.

### Government agency with environment separation

```yaml
institutional_sources:
  - label: "Production Analytics (read-only)"
    bucket: agency-analytics-prod
    prefix: reports/
    description: "Production analytics reports — approved for AI access"

  - label: "Sandbox Data (for testing)"
    bucket: agency-analytics-dev
    prefix: ""
    description: "Development and sandbox data — non-sensitive"
```

---

## Adding or Removing Sources

Simply edit `config/sources.yaml` and redeploy:

```bash
# Edit sources
nano config/sources.yaml

# Regenerate IAM policy and update Lambda environment
cdk deploy
```

**There is no live update mechanism** — changes only take effect after `cdk deploy`.
Lambda functions pick up the new policy immediately after the deploy completes.

---

## Validation Checklist

Before deploying, verify:

- [ ] Each `bucket` value is the bucket name only (no `s3://`, no trailing `/`)
- [ ] Prefixes end with `/` if intended to restrict to a path (e.g., `data/` not `data`)
- [ ] You have `s3:ListBucket` and `s3:GetObject` on each bucket (or the deploy will
  succeed but tool calls will return access-denied errors)
- [ ] `config/sources.yaml` is **not committed to a public repo** — it may contain
  bucket names that reveal your infrastructure

The `config/sources.yaml` is excluded from the example `.gitignore`. Keep it local
or use a private repo.

---

## Cross-Account Buckets

If your institutional buckets are in a different AWS account, add a bucket policy
on the source side that grants `s3:GetObject` and `s3:ListBucket` to the Lambda
execution role ARN. The Lambda role ARN is available as a CloudFormation output
after deploy:

```bash
aws cloudformation describe-stacks \
  --stack-name QuickSuiteOpenData \
  --query 'Stacks[0].Outputs[?contains(OutputKey,`S3ToolRole`)].OutputValue' \
  --output text
```
