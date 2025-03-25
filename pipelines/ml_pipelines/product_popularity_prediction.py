#!/usr/bin/env python
"""
Hybrid ML Pipeline with xorq Data Preparation and scikit-learn Modeling

This example demonstrates:
1. Using xorq for efficient data loading and preparation
2. Leveraging xorq's train_test_splits for declarative data splitting
3. Directly using scikit-learn for model training and prediction
4. Combining the strengths of xorq for data handling and scikit-learn for modeling

The pipeline predicts product popularity scores based on various features.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error

import xorq as xo
import xorq.vendor.ibis.expr.datatypes as dt
from xorq.ml import train_test_splits
from xorq.caching import ParquetStorage

# Path to our datasets
DATASETS_DIR = Path(__file__).parent.parent / "datasets"

# Define feature columns and target column
FEATURE_COLS = ["feature1", "feature2", "feature3", "weight"]
TARGET_COL = "popularity_score"
PREDICTED_COL = "predicted_score"


def main():
    # Create connection and setup caching
    con = xo.duckdb.connect()
    storage = ParquetStorage(source=con)

    # Step 1: Load and prepare data using xorq
    # Load product features dataset
    products = con.read_parquet(DATASETS_DIR / "product_features.parquet")

    # Select features and target for our ML model
    ml_data = products.select(
        ["product_id", "category", "price_tier"] + FEATURE_COLS + [TARGET_COL]
    ).cache(
        storage=storage
    )  # Cache intermediate results

    # Step 2: Use xorq's declarative train_test_split
    train_expr, test_expr = ml_data.pipe(
        train_test_splits,
        unique_key="product_id",
        test_sizes=(0.7, 0.3),  # 70% train, 30% test
        random_seed=42,
    )

    # Execute xorq expressions to get pandas DataFrames
    print("Fetching data for model training...")
    train_data = train_expr.execute()
    test_data = test_expr.execute()

    # Step 3: Train the model with scikit-learn
    print("Training RandomForest model using scikit-learn...")
    X_train = train_data[FEATURE_COLS]
    y_train = train_data[TARGET_COL]

    model = RandomForestRegressor(n_estimators=100, max_depth=5, random_state=42)
    model.fit(X_train, y_train)

    # Step 4: Make predictions with the trained model
    X_test = test_data[FEATURE_COLS]
    test_data[PREDICTED_COL] = model.predict(X_test)

    # Step 5: Evaluate model performance
    mse = mean_squared_error(test_data[TARGET_COL], test_data[PREDICTED_COL])
    rmse = np.sqrt(mse)

    print("\n=== Product Popularity Prediction Results ===")
    print(f"Mean Squared Error: {mse:.4f}")
    print(f"Root Mean Squared Error: {rmse:.4f}")
    print(f"\nFeature Importance:")

    # Display feature importance from scikit-learn model
    for feature, importance in zip(FEATURE_COLS, model.feature_importances_):
        print(f"  {feature}: {importance:.4f}")

    print("\nSample predictions (first 5 rows):")
    print(
        test_data[
            [
                "product_id",
                "category",
                "price_tier",
                TARGET_COL,
                PREDICTED_COL,
            ]
        ].head()
    )

    print(
        "\nThe ML pipeline used xorq for data preparation and scikit-learn for modeling."
    )
    print("This hybrid approach combines the best of both worlds: xorq's data handling")
    print("and scikit-learn's mature machine learning capabilities.")


if __name__ == "__main__":
    main()
