# xorg-mind

A collection of examples and resources for exploring the xorq data framework and its capabilities.

## Project Structure

- `pipelines/`: A collection of minimal, expressive pipeline examples demonstrating the key capabilities of xorq
  - `multi_engine/`: Examples of xorq's ability to work across multiple execution engines
  - `python_udfs/`: Demonstrations of User-Defined Functions in xorq pipelines
  - `ml_pipelines/`: Machine learning pipelines leveraging xorq's deferred execution and Arrow integration
  - `yaml_serialization/`: Examples of serializing and deserializing pipelines for reproducibility
  - `streaming_sentiment/`: Real-time streaming sentiment analysis using xorq, Kafka, and Hugging Face Transformers
  - `datasets/`: Sample datasets used by the examples
- `art/`: Medium article and supporting artifacts (coming)

## Key Features Demonstrated

1. **Deferred computation model** - Define pipelines declaratively, execute when needed
2. **Multi-engine execution** - Seamlessly move data between different engines (DuckDB, Snowflake, Python)
3. **Declarative pipeline definition** - Express pipelines in Python or YAML
4. **Expression serialization** - Serialize pipelines for reproducibility across environments
5. **Apache Arrow integration** - Leverage Arrow for high-performance data transfer
6. **Built-in caching** - Automatically cache intermediate results
7. **ML integration** - Use UDFs across execution engines for ML workflows
8. **Real-time streaming analytics** - Process streaming data with integration to Apache Kafka

## Getting Started

To run the pipeline examples:

```bash
# Run all examples
python pipelines/main.py

# Run a specific example
python pipelines/main.py multi_engine
python pipelines/main.py python_udfs
python pipelines/main.py ml_pipeline
python pipelines/main.py yaml_serialization
python pipelines/main.py streaming_sentiment
```

Each example is designed to be minimal yet expressive, showing the elegance and power of xorq's approach to data processing.

### Streaming Sentiment Analysis Pipeline

The streaming sentiment analysis pipeline demonstrates how to:

- Ingest real-time data from Kafka
- Process streaming text using xorq and DuckDB
- Apply sentiment analysis with Hugging Face Transformers
- Aggregate and analyze sentiment trends
- Visualize results in real-time with Streamlit

To run this pipeline:

1. Start Kafka: `cd pipelines/streaming_sentiment && docker-compose up -d`
2. Run data simulator: `python pipelines/streaming_sentiment/data_simulator.py`
3. Run the pipeline: `python pipelines/streaming_sentiment/streaming_sentiment_pipeline.py`
4. View the dashboard: `python pipelines/streaming_sentiment/streaming_sentiment_pipeline.py --dashboard`

For more details, see the [streaming_sentiment README](pipelines/streaming_sentiment/README.md).

## About xorq

xorq is a deferred computational framework that brings the replicability and performance of declarative pipelines to the Python ML ecosystem. It enables us to write pandas-style transformations that never run out of memory, automatically cache intermediate results, and seamlessly move between SQL engines and Python UDFs—all while maintaining replicability.
