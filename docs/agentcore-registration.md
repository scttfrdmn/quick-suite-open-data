# Registering Open Data Tools with AgentCore Gateway

After deploying the CDK stack, register the five tool Lambdas as AgentCore Gateway
Lambda targets so Quick Suite can discover and invoke them.

---

## Prerequisites

- `cdk deploy` completed successfully
- An AgentCore Gateway already exists (or you are creating one)
- You have IAM permissions to call `bedrock-agentcore:CreateTarget`

---

## Step 1 — Grant AgentCore Invoke Permission (pre-deploy)

The cleanest approach is to pass the Gateway execution role ARN **before** deploying
so the CDK stack auto-grants `lambda:InvokeFunction`:

```bash
cdk deploy --context agentcore_gateway_role_arn=arn:aws:iam::<ACCOUNT>:role/<GATEWAY_ROLE>
```

If you didn't do this at deploy time, add permissions manually after deploy:

```bash
# Retrieve all tool Lambda ARNs
aws cloudformation describe-stacks \
  --stack-name QuickSuiteOpenData \
  --query 'Stacks[0].Outputs[?OutputKey==`ToolArns`].OutputValue' \
  --output text | python3 -m json.tool
```

Then for each Lambda ARN:
```bash
aws lambda add-permission \
  --function-name <LAMBDA_ARN> \
  --statement-id AgentCoreInvoke \
  --action lambda:InvokeFunction \
  --principal bedrock-agentcore.amazonaws.com \
  --source-arn arn:aws:bedrock-agentcore:<REGION>:<ACCOUNT>:gateway/<GATEWAY_ID>
```

---

## Step 2 — Retrieve Lambda ARNs

```bash
# All tool ARNs as JSON
aws cloudformation describe-stacks \
  --stack-name QuickSuiteOpenData \
  --query 'Stacks[0].Outputs[?OutputKey==`ToolArns`].OutputValue' \
  --output text

# Individual ARNs by tool name
aws cloudformation describe-stacks \
  --stack-name QuickSuiteOpenData \
  --query 'Stacks[0].Outputs[?contains(OutputKey,`Arn`)].{Key:OutputKey,Value:OutputValue}' \
  --output table
```

Or use `scripts/post-deploy.sh` which prints all values with instructions.

---

## Step 3 — Register Lambda Targets via Console

In the AWS Console → Amazon Bedrock → AgentCore → Gateways → [your gateway]:

1. Click **Add target**
2. Select **Lambda function**
3. For each of the five tools:

| Target name | Lambda output key | Tool name in Quick Suite |
|------------|-------------------|--------------------------|
| `qs-open-data-roda-search` | `RodaSearchArn` | `roda_search` |
| `qs-open-data-roda-load` | `RodaLoadArn` | `roda_load` |
| `qs-open-data-s3-browse` | `S3BrowseArn` | `s3_browse` |
| `qs-open-data-s3-preview` | `S3PreviewArn` | `s3_preview` |
| `qs-open-data-s3-load` | `S3LoadArn` | `s3_load` |

For each target:
- **Lambda ARN**: paste the ARN from the CloudFormation output
- **Tool schema**: upload `config/roda-tools.yaml` (for roda_search, roda_load) or
  `config/s3-tools.yaml` (for s3_browse, s3_preview, s3_load)
- Leave auth as **IAM** (AgentCore Gateway uses its execution role)

---

## Step 4 — Register via AWS CLI

```bash
GATEWAY_ID="<YOUR_GATEWAY_ID>"
REGION="<YOUR_REGION>"

# Read ARNs
TOOL_ARNS=$(aws cloudformation describe-stacks \
  --stack-name QuickSuiteOpenData \
  --query 'Stacks[0].Outputs[?OutputKey==`ToolArns`].OutputValue' \
  --output text)

RODA_SEARCH_ARN=$(echo "$TOOL_ARNS" | python3 -c "import json,sys; print(json.load(sys.stdin)['roda_search'])")
RODA_LOAD_ARN=$(echo "$TOOL_ARNS" | python3 -c "import json,sys; print(json.load(sys.stdin)['roda_load'])")
S3_BROWSE_ARN=$(echo "$TOOL_ARNS" | python3 -c "import json,sys; print(json.load(sys.stdin)['s3_browse'])")
S3_PREVIEW_ARN=$(echo "$TOOL_ARNS" | python3 -c "import json,sys; print(json.load(sys.stdin)['s3_preview'])")
S3_LOAD_ARN=$(echo "$TOOL_ARNS" | python3 -c "import json,sys; print(json.load(sys.stdin)['s3_load'])")

# Register each target (repeat for each tool)
aws bedrock-agentcore create-gateway-target \
  --gateway-identifier "$GATEWAY_ID" \
  --name "qs-open-data-roda-search" \
  --target-configuration "{\"lambdaConfiguration\":{\"lambdaArn\":\"$RODA_SEARCH_ARN\"}}"
```

---

## Step 5 — Verify

Test each tool from the AgentCore Gateway console:

1. Go to your Gateway → Targets → select a target → **Test**
2. Send a sample payload:
   - `roda_search`: `{"query": "climate"}`
   - `s3_browse`: `{"source_label": "Shared Research Data"}`
3. Confirm a valid JSON response (not an error)

---

## Automation

`scripts/post-deploy.sh` prints all ARNs and the registration commands with your
actual values filled in. Run it after every `cdk deploy`.
