"""AWS client factory.

Reads an optional ``AWS_ENDPOINT_URL`` so the exact same code talks to
LocalStack in local dev and to real AWS in production — no code branch, just an
environment variable.
"""
from __future__ import annotations

import os

import boto3


def client(service: str):
    endpoint = os.environ.get("AWS_ENDPOINT_URL")  # set to LocalStack in dev
    region = os.environ.get("AWS_REGION", "us-east-1")
    kwargs = {"region_name": region}
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    return boto3.client(service, **kwargs)