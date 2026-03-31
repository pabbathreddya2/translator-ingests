#!/usr/bin/env python3
"""
Check if a source needs to be updated by comparing S3 build with upstream version.
Returns exit code 0 if update needed, 1 if can skip.
"""

import sys
import boto3
import json
from translator_ingest.pipeline import get_latest_source_version, get_transform_version
from translator_ingest.util.metadata import PipelineMetadata
from translator_ingest.normalize import get_current_node_norm_version


def check_s3_version(source: str, bucket: str = "kgx-translator-ingests") -> dict:
    """Check if source exists in S3 and get its version metadata."""
    s3 = boto3.client('s3')
    
    try:
        key = f"data/{source}/latest-build.json"
        response = s3.get_object(Bucket=bucket, Key=key)
        metadata = json.loads(response['Body'].read())
        return metadata
    except s3.exceptions.NoSuchKey:
        return None
    except Exception as e:
        print(f"  ! Error checking S3: {e}")
        return None


def main():
    if len(sys.argv) != 2:
        print("Usage: check_source_needs_update.py <source_name>")
        sys.exit(1)
    
    source = sys.argv[1]
    
    print(f"Checking if {source} needs update...")
    
    try:
        # Get upstream version using existing pipeline function
        upstream_version = get_latest_source_version(source)
        print(f"  Upstream version: {upstream_version}")
        
        # Create pipeline metadata and set all version components
        pipeline_metadata = PipelineMetadata(source, source_version=upstream_version)
        pipeline_metadata.transform_version = get_transform_version(source)
        pipeline_metadata.node_norm_version = get_current_node_norm_version()
        
        # Generate the build version from components
        current_build_version = pipeline_metadata.generate_build_version()
        
        # Check S3 for existing build
        s3_metadata = check_s3_version(source)
        
        if s3_metadata is None:
            print(f"  S3: No existing build found")
            print(f"  → {source} needs update")
            sys.exit(0)  # Exit 0 = needs processing
        
        s3_build_version = s3_metadata.get('build_version')
        print(f"  S3 build version: {s3_build_version}")
        print(f"  Current build version: {current_build_version}")
        
        # Compare build versions (includes source_version + transform_version + node_norm_version)
        if s3_build_version == current_build_version:
            print(f"  ✓ {source} is up to date")
            sys.exit(1)  # Exit 1 = skip processing
        else:
            print(f"  → {source} needs update (build versions differ)")
            sys.exit(0)  # Exit 0 = needs processing
            
    except Exception as e:
        print(f"  ! Error checking {source}: {e}")
        print(f"  → Assuming update needed to be safe")
        sys.exit(0)  # Exit 0 = needs processing (fail-safe)


if __name__ == "__main__":
    main()
