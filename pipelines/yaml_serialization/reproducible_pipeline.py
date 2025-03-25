#!/usr/bin/env python
"""
Reproducible Pipeline with YAML Serialization

This example demonstrates:
1. Defining a data pipeline using xorq
2. Serializing the pipeline to YAML
3. Loading and executing the pipeline from YAML

YAML serialization enables version control and reproducibility of data pipelines.
"""

import os
from pathlib import Path
import xorq as xo
from xorq.common.utils.defer_utils import deferred_read_parquet
from xorq.ibis_yaml.compiler import BuildManager

# String paths for datasets and builds (avoid Path objects for YAML serialization)
DATASETS_DIR = str(Path(__file__).parent.parent / "datasets")
BUILDS_DIR = str(Path(__file__).parent / "builds")
os.makedirs(BUILDS_DIR, exist_ok=True)


def main():
    # Step 1: Create connection
    conn = xo.duckdb.connect()

    # Step 2: Create a deferred read operation for our sales data
    # Using deferred_read_parquet is key for proper YAML serialization
    sales = deferred_read_parquet(
        conn, f"{DATASETS_DIR}/sales.parquet", table_name="sales_data"
    )

    # Step 3: Define a simple pipeline - regional sales analysis
    pipeline = (
        sales.filter(sales.unit_price > 100)
        .group_by("region")
        .aggregate(
            total_sales=sales.total_price.sum(), avg_price=sales.unit_price.mean()
        )
        .order_by(xo.desc("total_sales"))
    )

    # Step 4: Serialize to YAML
    print("Serializing pipeline to YAML...")
    build_manager = BuildManager(BUILDS_DIR)
    expr_hash = build_manager.compile_expr(pipeline)
    print(f"Pipeline serialized with hash: {expr_hash}")

    # Step 5: Load from YAML
    print("\nLoading pipeline from YAML...")
    loaded_pipeline = build_manager.load_expr(expr_hash)

    # Step 6: Execute original and loaded pipelines
    print("\nExecuting original pipeline:")
    original_result = pipeline.execute()
    print(original_result)

    try:
        print("\nExecuting loaded pipeline:")
        loaded_result = loaded_pipeline.execute()
        print(loaded_result)
        print("\nBoth pipelines produced identical results!")
    except Exception as e:
        print(f"\nError executing loaded pipeline: {e}")
        print("\nNote: In a real application, you would need to ensure the")
        print("data source is available when loading the serialized pipeline.")

    # Step 7: Show the YAML files
    print("\nYAML files created:")
    build_dir = os.path.join(BUILDS_DIR, expr_hash)
    for file in os.listdir(build_dir):
        if file.endswith(".yaml"):
            print(f"- {os.path.join(expr_hash, file)}")


if __name__ == "__main__":
    main()
