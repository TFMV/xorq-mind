#!/usr/bin/env python
"""
Real-time Streaming Sentiment Analysis Pipeline with xorq, Kafka, and Hugging Face

This example demonstrates:
1. Using xorq for efficient data processing in a streaming context
2. Integration with Apache Kafka for real-time data ingestion
3. Leveraging Hugging Face Transformers for sentiment analysis
4. Combining SQL-based processing with ML inference
5. Automatic caching of intermediate results
6. Real-time dashboard visualization with Streamlit

The pipeline ingests streaming social media comments, processes them,
performs sentiment analysis, and visualizes the results in real-time.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import xorq as xo
import xorq.expr.datatypes as dt
from xorq.caching import ParquetStorage
from xorq.udf import scalar
import time
import json
from datetime import datetime
from transformers import pipeline
from kafka import KafkaConsumer, KafkaProducer
import streamlit as st
import plotly.express as px
import threading
from queue import Queue
import re

# Define paths
CACHE_DIR = Path(__file__).parent / "cache"
CACHE_DIR.mkdir(exist_ok=True)

# Kafka configuration
KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC = "social_media_comments"
OUTPUT_TOPIC = "sentiment_results"

# Initialize sentiment analysis pipeline
sentiment_analyzer = pipeline(
    "sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english"
)


# Define helper functions for sentiment analysis
def preprocess_text(text):
    """Clean and preprocess text for sentiment analysis"""
    if not isinstance(text, str):
        return ""
    # Remove URLs
    text = re.sub(r"https?://\S+|www\.\S+", "", text)
    # Remove HTML tags
    text = re.sub(r"<.*?>", "", text)
    # Remove special characters
    text = re.sub(r"[^\w\s]", "", text)
    # Remove extra whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_sentiment(text):
    """Extract sentiment using Hugging Face Transformers"""
    if not text or len(text) < 3:
        return {"score": 0.5, "label": "NEUTRAL"}

    try:
        result = sentiment_analyzer(text)[0]
        return result
    except Exception as e:
        print(f"Error analyzing sentiment: {e}")
        return {"score": 0.5, "label": "NEUTRAL"}


def sentiment_score(text):
    """Return the sentiment score between -1 and 1"""
    if not text or len(text) < 3:
        return 0.0

    result = extract_sentiment(text)
    # Convert to a score between -1 and 1
    if result["label"] == "POSITIVE":
        return result["score"]
    elif result["label"] == "NEGATIVE":
        return -result["score"]
    return 0.0


# Define a simple analyze_sentiment function (no decorator needed)
def analyze_sentiment(text):
    """UDF for sentiment analysis compatible with xorq"""
    return sentiment_score(preprocess_text(text))


class StreamingPipeline:
    def __init__(self):
        # Create xorq connection and setup caching
        self.conn = xo.duckdb.connect()

        # Register our UDF with DuckDB directly using the DuckDB connection
        self.conn.con.create_function(
            "analyze_sentiment", analyze_sentiment, ["VARCHAR"], "DOUBLE"
        )

        # Create a caching layer for intermediate results
        self.storage = ParquetStorage(source=self.conn, path=CACHE_DIR)

        # Create Kafka consumer and producer
        self.consumer = KafkaConsumer(
            INPUT_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset="latest",
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )

        self.producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )

        # Queue for passing data between Kafka consumer and xorq processor
        self.data_queue = Queue(maxsize=1000)

        # Create an in-memory table for streaming data
        self.conn.con.execute(
            """
            CREATE TABLE IF NOT EXISTS comments (
                id VARCHAR,
                timestamp TIMESTAMP,
                user_id VARCHAR,
                platform VARCHAR,
                text VARCHAR,
                processed_at TIMESTAMP
            )
            """
        )

    def kafka_consumer_thread(self):
        """Thread function to consume messages from Kafka and add to queue"""
        for message in self.consumer:
            data = message.value
            # Add timestamp when processed
            data["processed_at"] = datetime.now().isoformat()
            self.data_queue.put(data)

    def process_batch(self, batch_data):
        """Process a batch of data using xorq"""
        # Convert batch to DataFrame and load into DuckDB
        df = pd.DataFrame(batch_data)

        # Insert data into comments table using xorq's proper approach
        self.conn.con.register("batch_df", df)
        self.conn.con.execute("INSERT INTO comments SELECT * FROM batch_df")

        # Use direct SQL to process the data with our UDF
        processed_sql = """
        SELECT 
            *,
            analyze_sentiment(text) AS sentiment,
            current_timestamp AS processed_timestamp
        FROM comments
        """
        # Execute SQL and convert to DataFrame
        processed_df = self.conn.con.execute(processed_sql).fetch_df()

        # Create a table reference to use for aggregations
        self.conn.con.execute(
            "CREATE OR REPLACE VIEW processed_comments AS " + processed_sql
        )
        processed = self.conn.table("processed_comments")

        # Step 2: Calculate aggregations using xorq's expressive API
        aggregated = processed.group_by(["platform"]).aggregate(
            avg_sentiment=processed.sentiment.mean(),
            count=processed.id.count(),
            positive_count=(
                xo.case().when(processed.sentiment > 0, 1).else_(0).end()
            ).sum(),
            negative_count=(
                xo.case().when(processed.sentiment < 0, 1).else_(0).end()
            ).sum(),
            neutral_count=(
                xo.case().when(processed.sentiment == 0, 1).else_(0).end()
            ).sum(),
        )

        # Execute the pipeline and get results
        results_df = aggregated.execute()

        # Convert results to JSON and send to Kafka
        for _, row in results_df.iterrows():
            self.producer.send(OUTPUT_TOPIC, value=row.to_dict())

        # Return the processed data DataFrame for visualization
        return processed_df

    def run(self):
        """Run the streaming pipeline"""
        # Start Kafka consumer thread
        consumer_thread = threading.Thread(target=self.kafka_consumer_thread)
        consumer_thread.daemon = True
        consumer_thread.start()

        print("Streaming pipeline started. Waiting for data...")

        # Process data in mini-batches
        while True:
            batch = []
            start_time = time.time()

            # Collect messages for up to 5 seconds or until we have 100 messages
            while time.time() - start_time < 5 and len(batch) < 100:
                try:
                    # Non-blocking get with timeout
                    data = self.data_queue.get(block=True, timeout=0.1)
                    batch.append(data)
                except:
                    # No data available within timeout
                    continue

            # Process the batch if we have data
            if batch:
                print(f"Processing batch of {len(batch)} messages")
                processed_df = self.process_batch(batch)

                # Print summary statistics
                sentiment_stats = processed_df["sentiment"].describe()
                print("\nSentiment Statistics:")
                print(sentiment_stats)

                # Sleep briefly to prevent high CPU usage
                time.sleep(0.1)


def create_streamlit_dashboard():
    """Create a Streamlit dashboard for real-time visualization"""
    st.title("Real-time Sentiment Analysis Dashboard")

    # Create a Kafka consumer for the output topic
    consumer = KafkaConsumer(
        OUTPUT_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset="latest",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    # Create placeholder for charts
    chart_placeholder = st.empty()
    stats_placeholder = st.empty()

    # Initialize data storage
    data = []

    # Update dashboard in real-time
    for message in consumer:
        # Add new data
        data.append(message.value)

        # Keep only last 100 records
        if len(data) > 100:
            data = data[-100:]

        # Convert to DataFrame
        df = pd.DataFrame(data)

        # Create visualization
        with chart_placeholder.container():
            if not df.empty:
                # Sentiment by platform
                fig = px.bar(
                    df,
                    x="platform",
                    y="avg_sentiment",
                    color="avg_sentiment",
                    color_continuous_scale="RdYlGn",
                    title="Average Sentiment by Platform",
                )
                st.plotly_chart(fig, use_container_width=True)

                # Volume by sentiment category
                volumes_df = df[
                    ["platform", "positive_count", "negative_count", "neutral_count"]
                ]
                volumes_df = volumes_df.melt(
                    id_vars=["platform"],
                    value_vars=["positive_count", "negative_count", "neutral_count"],
                    var_name="sentiment_type",
                    value_name="count",
                )

                fig2 = px.bar(
                    volumes_df,
                    x="platform",
                    y="count",
                    color="sentiment_type",
                    barmode="group",
                    title="Volume by Sentiment Category and Platform",
                )
                st.plotly_chart(fig2, use_container_width=True)

        # Display statistics
        with stats_placeholder.container():
            st.subheader("Summary Statistics")
            st.dataframe(df, use_container_width=True)

        # Short pause to prevent high CPU usage
        time.sleep(0.5)


def main():
    # Parse command-line arguments
    import argparse

    parser = argparse.ArgumentParser(description="Run streaming sentiment pipeline")
    parser.add_argument(
        "--dashboard", action="store_true", help="Run the Streamlit dashboard"
    )
    args = parser.parse_args()

    if args.dashboard:
        # Run the dashboard
        create_streamlit_dashboard()
    else:
        # Run the pipeline
        pipeline = StreamingPipeline()
        pipeline.run()


if __name__ == "__main__":
    main()
