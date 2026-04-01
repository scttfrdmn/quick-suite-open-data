#!/usr/bin/env bash
# post-deploy.sh — Quick Suite Open Data post-deployment configuration helper
#
# Run after `cdk deploy` to retrieve stack outputs and print the commands
# needed to register Lambda targets with AgentCore Gateway.
#
# Usage:
#   bash scripts/post-deploy.sh
#   bash scripts/post-deploy.sh --stack-name MyCustomStackName
#   bash scripts/post-deploy.sh --region us-west-2

set -euo pipefail

STACK_NAME="QuickSuiteOpenData"
REGION="${AWS_DEFAULT_REGION:-us-east-1}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --stack-name) STACK_NAME="$2"; shift 2 ;;
    --region)     REGION="$2"; shift 2 ;;
    *) echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done

echo ""
echo "========================================================"
echo "  Quick Suite Open Data — Post-Deploy Configuration"
echo "========================================================"
echo ""

echo "Fetching stack outputs from CloudFormation..."
OUTPUTS_JSON=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  --query 'Stacks[0].Outputs' \
  --output json 2>/dev/null) || {
    echo ""
    echo "ERROR: Could not retrieve stack '$STACK_NAME' in region '$REGION'."
    echo "  - Verify the stack deployed: cdk deploy"
    echo "  - Check region: export AWS_DEFAULT_REGION=<your-region>"
    exit 1
  }

get_output() {
  echo "$OUTPUTS_JSON" | python3 -c \
    "import json,sys; d={o['OutputKey']:o['OutputValue'] for o in json.load(sys.stdin)}; print(d.get('$1','<NOT_FOUND>'))"
}

TOOL_ARNS_JSON=$(get_output "ToolArns")
CATALOG_TABLE=$(get_output "CatalogTableName")
MANIFEST_BUCKET=$(get_output "ManifestBucketName")

echo ""
echo "--------------------------------------------------------"
echo "  Stack Outputs"
echo "--------------------------------------------------------"
echo "  Catalog DynamoDB table : $CATALOG_TABLE"
echo "  Manifest S3 bucket     : $MANIFEST_BUCKET"
echo ""
echo "  Tool Lambda ARNs (JSON):"
echo "  $TOOL_ARNS_JSON" | python3 -m json.tool 2>/dev/null || echo "  $TOOL_ARNS_JSON"
echo ""

RODA_SEARCH_ARN=$(echo "$TOOL_ARNS_JSON" | python3 -c "import json,sys; print(json.loads(sys.stdin.read())['roda_search'])" 2>/dev/null || echo "<roda_search_arn>")
RODA_LOAD_ARN=$(echo "$TOOL_ARNS_JSON" | python3 -c "import json,sys; print(json.loads(sys.stdin.read())['roda_load'])" 2>/dev/null || echo "<roda_load_arn>")
S3_BROWSE_ARN=$(echo "$TOOL_ARNS_JSON" | python3 -c "import json,sys; print(json.loads(sys.stdin.read())['s3_browse'])" 2>/dev/null || echo "<s3_browse_arn>")
S3_PREVIEW_ARN=$(echo "$TOOL_ARNS_JSON" | python3 -c "import json,sys; print(json.loads(sys.stdin.read())['s3_preview'])" 2>/dev/null || echo "<s3_preview_arn>")
S3_LOAD_ARN=$(echo "$TOOL_ARNS_JSON" | python3 -c "import json,sys; print(json.loads(sys.stdin.read())['s3_load'])" 2>/dev/null || echo "<s3_load_arn>")

echo "--------------------------------------------------------"
echo "  Step 1 of 2 — Register Lambda Targets with AgentCore Gateway"
echo "  Replace GATEWAY_ID with your AgentCore Gateway identifier."
echo "--------------------------------------------------------"
echo ""
echo "  GATEWAY_ID=\"<YOUR_GATEWAY_ID>\""
echo ""
echo "  aws bedrock-agentcore create-gateway-target \\"
echo "    --gateway-identifier \"\$GATEWAY_ID\" \\"
echo "    --name qs-open-data-roda-search \\"
echo "    --target-configuration '{\"lambdaConfiguration\":{\"lambdaArn\":\"$RODA_SEARCH_ARN\"}}'"
echo ""
echo "  aws bedrock-agentcore create-gateway-target \\"
echo "    --gateway-identifier \"\$GATEWAY_ID\" \\"
echo "    --name qs-open-data-roda-load \\"
echo "    --target-configuration '{\"lambdaConfiguration\":{\"lambdaArn\":\"$RODA_LOAD_ARN\"}}'"
echo ""
echo "  aws bedrock-agentcore create-gateway-target \\"
echo "    --gateway-identifier \"\$GATEWAY_ID\" \\"
echo "    --name qs-open-data-s3-browse \\"
echo "    --target-configuration '{\"lambdaConfiguration\":{\"lambdaArn\":\"$S3_BROWSE_ARN\"}}'"
echo ""
echo "  aws bedrock-agentcore create-gateway-target \\"
echo "    --gateway-identifier \"\$GATEWAY_ID\" \\"
echo "    --name qs-open-data-s3-preview \\"
echo "    --target-configuration '{\"lambdaConfiguration\":{\"lambdaArn\":\"$S3_PREVIEW_ARN\"}}'"
echo ""
echo "  aws bedrock-agentcore create-gateway-target \\"
echo "    --gateway-identifier \"\$GATEWAY_ID\" \\"
echo "    --name qs-open-data-s3-load \\"
echo "    --target-configuration '{\"lambdaConfiguration\":{\"lambdaArn\":\"$S3_LOAD_ARN\"}}'"
echo ""

echo "--------------------------------------------------------"
echo "  Step 2 of 2 — Pre-authorize AgentCore invoke (if not done at deploy)"
echo "  Skip this if you deployed with --context agentcore_gateway_role_arn=..."
echo "--------------------------------------------------------"
echo ""
echo "  For each tool Lambda, add invoke permission:"
echo "  aws lambda add-permission \\"
echo "    --function-name <LAMBDA_ARN> \\"
echo "    --statement-id AgentCoreInvoke \\"
echo "    --action lambda:InvokeFunction \\"
echo "    --principal bedrock-agentcore.amazonaws.com"
echo ""
echo "  Full details: docs/agentcore-registration.md"
echo ""
echo "========================================================"
echo "  Done."
echo "========================================================"
echo ""
