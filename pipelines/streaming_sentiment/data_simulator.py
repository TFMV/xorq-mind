#!/usr/bin/env python
"""
Data Simulator for Streaming Sentiment Analysis Pipeline

This script generates simulated social media comments and sends them to a Kafka topic
to be processed by the streaming_sentiment_pipeline.py script.

It creates realistic-looking comments across different platforms with varying sentiment
to demonstrate the capabilities of the xorq-based streaming analytics pipeline.
"""

import json
import time
import random
import uuid
from datetime import datetime
from kafka import KafkaProducer
import argparse

# Sample data for generating realistic-looking comments
PLATFORMS = ["Twitter", "Facebook", "Instagram", "Reddit", "LinkedIn"]

POSITIVE_TEMPLATES = [
    "I love the new {product}! It's amazing!",
    "Just tried {product} and I'm impressed with the quality.",
    "The team at {company} has done an excellent job with their customer service.",
    "Wow, {company}'s latest release exceeds all my expectations!",
    "I'm really happy with my purchase from {company}.",
    "This {product} has made my life so much easier!",
    "Great experience with {company} today, highly recommended!",
    "The new features in {product} are exactly what I needed.",
    "Thank you {company} for the excellent support!",
    "I can't believe how good the new {product} is!",
]

NEGATIVE_TEMPLATES = [
    "I'm disappointed with the {product}, it doesn't work as advertised.",
    "Poor customer service from {company}, waited hours for a response.",
    "The quality of {product} has really gone downhill lately.",
    "I regret buying from {company}, not worth the money.",
    "The {product} broke after just one week of use.",
    "Terrible experience with {company}, would not recommend.",
    "I'm frustrated with the lack of features in {product}.",
    "The {product} is overpriced and underperforms.",
    "{company} has the worst customer support I've ever dealt with.",
    "I'm returning my {product}, completely unsatisfied.",
]

NEUTRAL_TEMPLATES = [
    "Just got the new {product}, will see how it performs.",
    "Has anyone tried {company}'s new services?",
    "Looking for feedback on the {product} before I buy it.",
    "I wonder if {company} will address these issues in their next update.",
    "Comparing {product} with competitors before making a decision.",
    "Not sure if I should upgrade to the new {product}.",
    "Anyone have experience with {company}?",
    "What's the average price for {product} these days?",
    "Thinking about trying {company}'s services.",
    "Is the {product} compatible with my current setup?",
]

PRODUCTS = [
    "smartphone",
    "laptop",
    "headphones",
    "smartwatch",
    "camera",
    "fitness tracker",
    "wireless earbuds",
    "tablet",
    "smart speaker",
    "TV",
    "gaming console",
    "coffee maker",
    "blender",
    "drone",
]

COMPANIES = [
    "Apple",
    "Samsung",
    "Google",
    "Microsoft",
    "Amazon",
    "Sony",
    "Dell",
    "HP",
    "Lenovo",
    "Bose",
    "Nike",
    "Adidas",
    "Tesla",
    "Dyson",
    "LG",
    "Philips",
]


def generate_comment(sentiment_bias=None):
    """Generate a random social media comment with optional sentiment bias"""
    # Select sentiment based on bias or randomly
    if sentiment_bias is None:
        sentiment_weights = [0.4, 0.3, 0.3]  # positive, negative, neutral
    elif sentiment_bias == "positive":
        sentiment_weights = [0.7, 0.1, 0.2]
    elif sentiment_bias == "negative":
        sentiment_weights = [0.1, 0.7, 0.2]
    else:  # neutral
        sentiment_weights = [0.2, 0.2, 0.6]

    sentiment = random.choices(
        ["positive", "negative", "neutral"], weights=sentiment_weights
    )[0]

    # Select template based on sentiment
    if sentiment == "positive":
        template = random.choice(POSITIVE_TEMPLATES)
    elif sentiment == "negative":
        template = random.choice(NEGATIVE_TEMPLATES)
    else:
        template = random.choice(NEUTRAL_TEMPLATES)

    # Fill in template with random products and companies
    text = template.format(
        product=random.choice(PRODUCTS), company=random.choice(COMPANIES)
    )

    # Generate comment metadata
    comment = {
        "id": str(uuid.uuid4()),
        "timestamp": datetime.now().isoformat(),
        "user_id": f"user_{random.randint(1000, 9999)}",
        "platform": random.choice(PLATFORMS),
        "text": text,
    }

    return comment


def simulate_data(kafka_broker, topic, rate=1.0, duration=None, sentiment_bias=None):
    """
    Simulate data and send to Kafka topic

    Args:
        kafka_broker: Kafka broker address
        topic: Kafka topic to send data to
        rate: Messages per second (average)
        duration: How long to run in seconds (None for indefinite)
        sentiment_bias: Bias the sentiment of messages (None, 'positive', 'negative', 'neutral')
    """
    try:
        # Create Kafka producer
        producer = KafkaProducer(
            bootstrap_servers=[kafka_broker],
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )

        print(f"Starting data simulation, sending to topic: {topic}")
        print(f"Rate: {rate} messages per second")
        if duration:
            print(f"Will run for {duration} seconds")
        if sentiment_bias:
            print(f"Sentiment bias: {sentiment_bias}")

        start_time = time.time()
        count = 0

        # Run until duration (if specified)
        while duration is None or time.time() - start_time < duration:
            # Generate and send comment
            comment = generate_comment(sentiment_bias)
            producer.send(topic, value=comment)

            count += 1
            if count % 10 == 0:
                print(f"Sent {count} messages. Latest: {comment['text'][:50]}...")

            # Sleep to maintain desired rate
            time.sleep(1.0 / rate)

        print(
            f"Simulation complete. Sent {count} messages in {time.time() - start_time:.2f} seconds"
        )

    except KeyboardInterrupt:
        print("\nSimulation stopped by user")
    except Exception as e:
        print(f"Error in data simulation: {e}")
    finally:
        if "producer" in locals():
            producer.close()


def main():
    parser = argparse.ArgumentParser(
        description="Simulate social media comments for streaming pipeline"
    )
    parser.add_argument(
        "--broker", default="localhost:9092", help="Kafka broker address"
    )
    parser.add_argument(
        "--topic", default="social_media_comments", help="Kafka topic to send data to"
    )
    parser.add_argument("--rate", type=float, default=2.0, help="Messages per second")
    parser.add_argument(
        "--duration", type=int, help="Duration in seconds (default: run indefinitely)"
    )
    parser.add_argument(
        "--sentiment",
        choices=["positive", "negative", "neutral"],
        help="Bias the sentiment of generated data",
    )

    args = parser.parse_args()

    simulate_data(
        args.broker,
        args.topic,
        rate=args.rate,
        duration=args.duration,
        sentiment_bias=args.sentiment,
    )


if __name__ == "__main__":
    main()
