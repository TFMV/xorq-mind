# Real-time Streaming Sentiment Analysis

This pipeline demonstrates integration of xorq with Apache Kafka and Hugging Face Transformers for real-time sentiment analysis of streaming social media data. The pipeline showcases xorq's ability to process streaming data efficiently while leveraging machine learning models for sentiment analysis.

## Key Features

1. **Real-time data processing** - Process streaming social media comments as they arrive
2. **Multi-engine execution** - Use xorq's DuckDB backend for data processing and Hugging Face for machine learning
3. **Sentiment analysis** - Analyze text sentiment using state-of-the-art transformer models
4. **Real-time visualization** - Display results in an interactive Streamlit dashboard
5. **SQL-based processing** - Leverage DuckDB's SQL capabilities within xorq
6. **Python UDFs** - Extend xorq with custom Python functions for sentiment analysis

## Architecture

The pipeline consists of three main components:

1. **Data Simulator** - Generates realistic social media comments and sends them to a Kafka topic
2. **Streaming Pipeline** - Consumes from Kafka, processes data with xorq and DuckDB, and produces results
3. **Dashboard** - Visualizes real-time sentiment analysis results

## Prerequisites

- Python 3.8+
- Apache Kafka
- Docker (recommended for Kafka setup)

## Setup

1. **Install Dependencies**

```bash
pip install xorq kafka-python transformers torch pandas numpy streamlit plotly
```

2. **Start Kafka**

Using Docker Compose:

```bash
docker-compose up -d
```

Or use an existing Kafka instance by updating the broker address in the scripts.

3. **Create Kafka Topics**

```bash
docker exec -it kafka kafka-topics.sh --create --topic social_media_comments --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec -it kafka kafka-topics.sh --create --topic sentiment_results --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

## Running the Pipeline

1. **Start the Data Simulator**

```bash
python data_simulator.py --rate 2
```

This will generate 2 simulated social media comments per second. You can adjust the rate, duration, or sentiment bias:

```bash
python data_simulator.py --rate 5 --duration 300 --sentiment positive
```

2. **Run the Sentiment Analysis Pipeline**

```bash
python streaming_sentiment_pipeline.py
```

This will start consuming data from Kafka, process it with xorq, and produce results.

3. **Start the Dashboard**

In a separate terminal:

```bash
python streaming_sentiment_pipeline.py --dashboard
```

Or use Streamlit directly:

```bash
streamlit run streaming_sentiment_pipeline.py -- --dashboard
```

## Implementation Details

Our implementation uses several xorq features:

- **DuckDB Integration**: We use xorq's DuckDB backend for efficient data processing
- **Custom UDFs**: We register a sentiment analysis function with DuckDB
- **Hybrid SQL/Ibis Approach**: We combine direct SQL for UDF execution with xorq's expressive API for aggregations
- **Mini-Batch Processing**: Data is processed in configurable mini-batches for efficiency
- **Aggregation Pipelines**: We use xorq's group_by and aggregate functions for statistical analysis

## How It Works

1. **Data Generation**: The simulator creates realistic-looking social media comments and sends them to Kafka.
2. **Ingestion**: The xorq pipeline consumes messages from Kafka and processes them in mini-batches.
3. **Preprocessing & Analysis**:
   - Messages are stored in a DuckDB table
   - SQL with our custom UDF performs sentiment analysis
   - Results are stored in a view for further processing
4. **Aggregation**: xorq aggregates results by platform using its expressive API:
   - Average sentiment score
   - Counts of positive, negative, and neutral comments
5. **Output**: Results are sent to another Kafka topic for the dashboard to consume.
6. **Visualization**: The Streamlit dashboard presents real-time visualizations of sentiment trends.

## Extending the Pipeline

This pipeline demonstrates a powerful architecture that can be extended in several ways:

- **Add more UDFs**: Create additional text analysis functions (entity extraction, topic modeling)
- **Integrate additional ML models**: Add more complex NLP processing
- **Connect to real data sources**: Replace the simulator with real social media APIs
- **Add alerting**: Trigger alerts when sentiment drops below thresholds
- **Store historical data**: Save results to a database for long-term analysis

## Troubleshooting

- **Kafka Connection Issues**: Ensure Kafka is running and accessible at the specified broker address
- **Missing Dependencies**: Make sure all required packages are installed
- **UDF Registration**: If experiencing UDF issues, check the function signature and parameter types
- **Performance Issues**: Adjust batch sizes and processing intervals for optimal performance
