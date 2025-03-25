# xorq Pipeline Examples

This directory contains minimal, expressive examples demonstrating the key capabilities of xorq, the multi-engine ML pipeline framework. Each example showcases different aspects of xorq's design and functionality.

## Directory Structure

- `multi_engine/`: Examples of xorq's ability to work across multiple execution engines
- `python_udfs/`: Demonstrations of User-Defined Functions in xorq pipelines
- `ml_pipelines/`: Machine learning pipelines leveraging xorq's deferred execution and Arrow integration
- `yaml_serialization/`: Examples of serializing and deserializing pipelines for reproducibility
- `datasets/`: Sample datasets used by the examples

## Key Features Demonstrated

1. **Deferred computation model** - Define pipelines declaratively, execute when needed
2. **Multi-engine execution** - Seamlessly move data between different engines (DuckDB, Snowflake, Python)
3. **Declarative pipeline definition** - Express pipelines in Python or YAML
4. **Expression serialization** - Serialize pipelines for reproducibility across environments
5. **Apache Arrow integration** - Leverage Arrow for high-performance data transfer
6. **Built-in caching** - Automatically cache intermediate results
7. **ML integration** - Use UDFs across execution engines for ML workflows

## Getting Started

Each directory contains standalone examples that can be run individually. The examples are designed to be minimal yet expressive, showing actual usage patterns rather than just API demonstrations.

```bash
# Example: Run a multi-engine pipeline
python pipelines/multi_engine/duckdb_to_snowflake.py

# Example: Run a pipeline with Python UDFs
python pipelines/python_udfs/cached_udf_pipeline.py
```
