#!/usr/bin/env python3
import aws_cdk as cdk

from stacks.open_data_stack import OpenDataStack

app = cdk.App()
OpenDataStack(
    app,
    "QuickSuiteOpenData",
    description=(
        "Quick Suite Open Data — RODA public datasets and institutional S3 "
        "browser as AgentCore Gateway Lambda targets."
    ),
)
app.synth()
