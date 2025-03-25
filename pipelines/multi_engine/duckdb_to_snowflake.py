#!/usr/bin/env python
"""
Multi-Engine Pipeline Example: DuckDB to Snowflake (simulated)

This example demonstrates xorq's ability to seamlessly orchestrate data movement
between different execution engines. It:

1. Loads two datasets into DuckDB
2. Performs a join operation in DuckDB
3. Sends the results to another backend (simulated Snowflake)
4. Performs final aggregations in the second backend

The magic of xorq is that all data movement between engines happens automatically,
with minimal code, and the entire pipeline is defined declaratively.
"""

import xorq as xo
from pathlib import Path
from xorq.expr.relations import into_backend

# Path to our datasets
DATASETS_DIR = Path(__file__).parent.parent / "datasets"


def main():
    # Create connections to different engines
    # In a real scenario, the second connection would be to Snowflake
    # For demonstration, we use two separate DuckDB connections
    duckdb_conn = xo.duckdb.connect()
    snowflake_conn = xo.duckdb.connect()  # In real use: xo.snowflake.connect()

    # Load datasets into DuckDB
    sales = duckdb_conn.read_parquet(DATASETS_DIR / "sales.parquet")
    customers = duckdb_conn.read_parquet(DATASETS_DIR / "customers.parquet")

    # Step 1: Filter sales in DuckDB
    filtered_sales = sales.filter([sales.quantity > 2, sales.unit_price > 100])

    # Step 2: Join with customer data in DuckDB
    enriched_sales = filtered_sales.join(
        customers, predicates="customer_id", how="inner"  # Join on customer_id
    )

    # Step 3: Move computation to "Snowflake" via into_backend
    # This is where xorq's multi-engine magic happens - one line of code!
    snowflake_data = into_backend(
        enriched_sales[["customer_id", "segment", "total_price", "region"]],
        snowflake_conn,
        "high_value_sales",
    )

    # Step 4: Perform aggregation in "Snowflake"
    result = (
        snowflake_data.group_by(["segment", "region"])
        .aggregate(
            total_sales=snowflake_data.total_price.sum(),
            avg_sale=snowflake_data.total_price.mean(),
            customer_count=snowflake_data.customer_id.nunique(),
        )
        .order_by(xo.desc("total_sales"))
    )

    # Execute the pipeline
    # xorq defers execution until this point
    df = result.execute()

    print("\n=== Sales Analysis by Customer Segment and Region ===")
    print(df)
    print(
        "\nNotice how the entire pipeline was defined declaratively, then executed at once."
    )
    print("xorq orchestrated all data movement between engines automatically.")


if __name__ == "__main__":
    main()
