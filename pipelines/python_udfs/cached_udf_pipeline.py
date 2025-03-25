#!/usr/bin/env python
"""
Python UDF Pipeline with Caching

This example demonstrates:
1. Creating and using Python UDFs in xorq pipelines
2. Automatic caching of intermediate results
3. Deferred computation (define now, execute later)

UDFs allow you to extend xorq with custom Python functions while
maintaining the declarative nature of the pipeline. Caching ensures
expensive operations are performed only once.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import xorq as xo
import xorq.expr.datatypes as dt
from xorq.caching import ParquetStorage


# Path to our datasets
DATASETS_DIR = Path(__file__).parent.parent / "datasets"


def calculate_value_ratio(total_spent, lifetime_value):
    """Calculate ratio of total spent to lifetime value"""
    return total_spent / lifetime_value


def assign_value_tier(value_ratio):
    """Assign a value tier based on the value ratio"""
    conditions = [
        value_ratio < 0.1,
        value_ratio < 0.25,
        value_ratio < 0.5,
        value_ratio < 0.75,
    ]
    choices = ["Low", "Medium", "High", "Very High"]
    return np.select(conditions, choices, default="Top")


def main():
    # Create connection and setup caching
    conn = xo.duckdb.connect()

    # Register UDFs with DuckDB directly
    conn.con.create_function(
        "calculate_value_ratio", calculate_value_ratio, ["DOUBLE", "DOUBLE"], "DOUBLE"
    )
    conn.con.create_function(
        "assign_value_tier", assign_value_tier, ["DOUBLE"], "VARCHAR"
    )

    # Create a caching layer for intermediate results
    # This is where xorq's magic happens - caching is declarative!
    storage = ParquetStorage(source=conn, path=Path(__file__).parent / "cache")

    # Load datasets
    sales = conn.read_parquet(DATASETS_DIR / "sales.parquet")
    customers = conn.read_parquet(DATASETS_DIR / "customers.parquet")

    # Step 1: Calculate total spent by customer
    # The .cache() call tells xorq to store this intermediate result
    customer_totals = (
        sales.group_by("customer_id")
        .aggregate(total_spent=sales.total_price.sum())
        .cache(storage=storage)  # Cache this intermediate result
    )

    # Step 2: Join with customer data
    customer_analysis = customer_totals.join(
        customers, predicates="customer_id", how="inner"
    )

    # Step 3: Apply our Python UDFs
    # This is where we extend the pipeline with custom logic

    # First, create the intermediate table with value_ratio
    with_ratio = customer_analysis.mutate(
        # Apply our first UDF to calculate value ratio
        value_ratio=calculate_value_ratio(
            customer_analysis.total_spent, customer_analysis.lifetime_value
        )
    )

    # Now use that intermediate table for the second UDF
    result = (
        with_ratio.mutate(
            # Apply our second UDF to assign a tier using the calculated value_ratio
            value_tier=assign_value_tier(with_ratio.value_ratio)
        )
        .select(
            [
                "customer_id",
                "name",
                "segment",
                "total_spent",
                "lifetime_value",
                "value_ratio",
                "value_tier",
            ]
        )
        .order_by(xo.desc("value_ratio"))
        .limit(10)
    )

    # Execute the pipeline - xorq defers execution until this point
    df = result.execute()

    print("\n=== Top 10 Customers by Value Ratio ===")
    print(df)
    print("\nThe pipeline used Python UDFs to extend functionality while")
    print("maintaining the declarative style and leveraging automatic caching.")


if __name__ == "__main__":
    main()
