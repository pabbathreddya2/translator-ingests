#!/usr/bin/env python3
"""
Check if a source needs to be updated by comparing existing build with upstream version.
Reuses existing pipeline.py functions for consistency.
Returns exit code 0 if update needed, 1 if can skip.
"""

import sys
from translator_ingest.pipeline import get_latest_source_version, is_latest_build_metadata_current
from translator_ingest.util.metadata import PipelineMetadata


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
        
        # Create pipeline metadata with upstream version
        pipeline_metadata = PipelineMetadata(source, source_version=upstream_version)
        
        # Check if this version is already built
        is_current = is_latest_build_metadata_current(pipeline_metadata)
        
        if is_current:
            print(f"  ✓ {source} is up to date (version {upstream_version} already built)")
            sys.exit(1)  # Exit 1 = skip processing
        else:
            print(f"  → {source} needs update (new version {upstream_version})")
            sys.exit(0)  # Exit 0 = needs processing
    except Exception as e:
        print(f"  ! Error checking {source}: {e}")
        print(f"  → Assuming update needed to be safe")
        sys.exit(0)  # Exit 0 = needs processing (fail-safe)


if __name__ == "__main__":
    main()
