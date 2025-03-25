#!/usr/bin/env python
"""
Dataset setup script for xorq pipeline examples.
This script downloads and prepares sample datasets used in the examples.
"""

import os
import pandas as pd
import numpy as np
from pathlib import Path

# Ensure datasets directory exists
DATASETS_DIR = Path(__file__).parent
DATASETS_DIR.mkdir(exist_ok=True)


def create_sales_data():
    """Create a sample sales dataset"""
    np.random.seed(42)
    n = 1000

    # Create sales data
    data = {
        "date": pd.date_range(start="2023-01-01", periods=n),
        "product_id": np.random.randint(1, 100, size=n),
        "customer_id": np.random.randint(1, 500, size=n),
        "quantity": np.random.randint(1, 10, size=n),
        "unit_price": np.random.uniform(10, 1000, size=n).round(2),
        "region": np.random.choice(["North", "South", "East", "West"], size=n),
    }

    # Calculate total_price
    df = pd.DataFrame(data)
    df["total_price"] = (df["quantity"] * df["unit_price"]).round(2)

    # Save to parquet
    sales_path = DATASETS_DIR / "sales.parquet"
    df.to_parquet(sales_path)
    print(f"Created sales dataset at {sales_path}")
    return df


def create_customer_data(sales_df):
    """Create a sample customer dataset"""
    # Extract unique customer IDs from sales data
    customer_ids = sales_df["customer_id"].unique()
    n = len(customer_ids)

    # Create customer data
    data = {
        "customer_id": customer_ids,
        "name": [f"Customer {i}" for i in range(n)],
        "email": [f"customer{i}@example.com" for i in range(n)],
        "signup_date": pd.date_range(start="2022-01-01", periods=n),
        "lifetime_value": np.random.uniform(1000, 50000, size=n).round(2),
        "segment": np.random.choice(["Premium", "Standard", "Basic"], size=n),
    }

    df = pd.DataFrame(data)

    # Save to parquet
    customer_path = DATASETS_DIR / "customers.parquet"
    df.to_parquet(customer_path)
    print(f"Created customer dataset at {customer_path}")
    return df


def create_product_features():
    """Create a product features dataset for ML examples"""
    np.random.seed(42)
    n = 100  # 100 products

    # Create product features
    data = {
        "product_id": list(range(1, n + 1)),
        "category": np.random.choice(
            ["Electronics", "Clothing", "Home", "Books", "Sports"], size=n
        ),
        "weight": np.random.uniform(0.1, 20, size=n).round(2),
        "price_tier": np.random.choice(
            ["Budget", "Mid-range", "Premium", "Luxury"], size=n
        ),
        "feature1": np.random.normal(0, 1, size=n),
        "feature2": np.random.normal(0, 1, size=n),
        "feature3": np.random.normal(0, 1, size=n),
        "popularity_score": np.random.uniform(1, 10, size=n).round(2),
    }

    df = pd.DataFrame(data)

    # Save to parquet
    product_path = DATASETS_DIR / "product_features.parquet"
    df.to_parquet(product_path)
    print(f"Created product features dataset at {product_path}")
    return df


def main():
    print("Setting up datasets for xorq pipeline examples...")
    sales_df = create_sales_data()
    create_customer_data(sales_df)
    create_product_features()
    print("Dataset setup complete!")


if __name__ == "__main__":
    main()
