#!/usr/bin/env python
"""
xorq Pipeline Examples Runner

This script provides an easy way to run the different pipeline examples
included in this project. It first sets up the example datasets, then
allows running one or all of the pipeline examples.
"""

import os
import sys
import importlib
from pathlib import Path
import argparse

# Define the example modules
EXAMPLES = {
    "multi_engine": "multi_engine.duckdb_to_snowflake",
    "python_udfs": "python_udfs.cached_udf_pipeline",
    "ml_pipeline": "ml_pipelines.product_popularity_prediction",
    "yaml_serialization": "yaml_serialization.reproducible_pipeline",
}


def setup_datasets():
    """Set up the example datasets"""
    print("\n=== Setting up example datasets ===\n")
    dataset_setup = importlib.import_module("datasets.setup_datasets")
    dataset_setup.main()


def run_example(example_name):
    """Run a specific example"""
    if example_name not in EXAMPLES:
        print(f"Error: Example '{example_name}' not found!")
        print(f"Available examples: {', '.join(EXAMPLES.keys())}")
        return False

    print(f"\n=== Running {example_name} example ===\n")

    # Import and run the example module
    example_module = importlib.import_module(EXAMPLES[example_name])
    example_module.main()

    return True


def run_all_examples():
    """Run all the examples"""
    for example_name in EXAMPLES:
        print("\n" + "=" * 70)
        run_example(example_name)
        print("\n" + "=" * 70)
        input("Press Enter to continue to the next example...")


def main():
    parser = argparse.ArgumentParser(description="Run xorq pipeline examples")
    parser.add_argument(
        "example",
        nargs="?",
        choices=list(EXAMPLES.keys()) + ["all"],
        default="all",
        help="The example to run (default: run all examples)",
    )

    args = parser.parse_args()

    # Make sure we're in the right directory
    script_dir = Path(__file__).parent
    os.chdir(script_dir)

    # Add the current directory to sys.path so we can import modules
    sys.path.insert(0, str(script_dir))

    # Setup the datasets first
    setup_datasets()

    # Run the examples
    if args.example == "all":
        run_all_examples()
    else:
        run_example(args.example)

    print("\nAll examples completed!")


if __name__ == "__main__":
    main()
