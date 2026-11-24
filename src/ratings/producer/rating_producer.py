import json
import os
import random
import time
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "movie-ratings")

def build_event():
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 5000),
        "movie_id": random.randint(1, 1000),
        "rating": random.choice([0.5, 1.0, 2.0, 3.0, 3.5, 4.0, 4.5, 5.0]),
        "event_ts": datetime.now(timezone.utc).isoformat(),
    }

def main():
    producer = KafkaProducer(
        bootstrap_servers=BROKERS.split(","),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        retries=5,
        acks="all",
    )
    print(f"Producing rating events to {TOPIC} via {BROKERS}")
    try:
        while True:
            event = build_event()
            producer.send(TOPIC, key=event["event_id"].encode(), value=event)
            producer.flush()
            print(event)
            time.sleep(1)
    except KeyboardInterrupt:
        print("Producer stopped")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
