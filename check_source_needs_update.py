#!/usr/bin/env python3
"""
Check if a source needs to be updated by comparing local/S3 versions with upstream.
Returns exit code 0 if update needed, 1 if can skip.
"""

import sys
import boto3
import json
from pathlib import Path
from importlib import import_module

def check_s3_version(source: str, bucket: str = "kgx-translator-ingests") -> dict:
    """Check if source exists in S3 and get its version metadata."""
    s3 = boto3.client('s3')
    
    try:
        # Check for latest-build.json in S3
        key = f"data/{source}/latest-build.json"
        response = s3.get_object(Bucket=bucket, Key=key)
        metadata = json.loads(response['Body'].read())
        return metadata
    except s3.exceptions.NoSuchKey:
        return None
    except Exception as e:
        print(f"Error checking S3: {e}", file=sys.stderr)
        return None


def get_upstream_version(source: str) -> str:
    """Get the latest upstream version for a source."""
    try:
        # Import the source's module to get version
        module = import_module(f"translator_ingest.ingests.{source}.{source}")
        
        if hasattr(module, 'get_latest_version'):
            version = module.get_latest_version()
            return version
        else:
            # No version detection, assume needs update
            return "unknown"
    except Exception as e:
        print(f"Error getting upstream version: {e}", file=sys.stderr)
        return "unknown"


def main():
    if len(sys.argv) != 2:
        print("Usage: check_source_needs_update.py <source_name>")
        sys.exit(1)
    
    source = sys.argv[1]
    
    print(f"Checking if {source} needs update...")
    
    # Get upstream version
    upstream_version = get_upstream_version(source)
    print(f"  Upstream version: {upstream_version}")
    
    # Check S3 for existing build
    s3_metadata = check_s3_version(source)
    
    if s3_metadata is None:
        print(f"  S3: No existing build found")
        print(f"  Decision: UPDATE NEEDED")
        sys.exit(0)  # Need to run
    
    s3_version = s3_metadata.get('source_version', 'unknown')
    print(f"  S3 version: {s3_version}")
    
    # Compare versions
    if upstream_version == "unknown":
        # Can't determine, run to be safe
        print(f"  Decision: UPDATE NEEDED (version unknown)")
        sys.exit(0)
    
    if s3_version == upstream_version:
        print(f"  Decision: SKIP (versions match)")
        sys.exit(1)  # Skip
    else:
        print(f"  Decision: UPDATE NEEDED (versions differ)")
        sys.exit(0)  # Need to run


if __name__ == "__main__":
    main()
