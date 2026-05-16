import json
import time
import random
from confluent_kafka import Producer

KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
TOPIC = "raw-posts"

producer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS
}
producer = Producer(producer_conf)

def generate_synthetic_post():
    hashtags = ["#bluesky", "#ai", "#coding", "#testing", "#realtime"]
    text = f"Synthetic test post with {random.choice(hashtags)} and {random.choice(hashtags)}"
    return {
        "post_id": f"test-{random.randint(0, 1000000)}",
        "author_did": "did:plc:test",
        "text": text,
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "langs": ["en"],
        "reply": False,
        "embed_type": None,
        "ingested_at": time.strftime("%Y-%m-%dT%H:%M:%SZ")
    }

def main():
    print(f"Producing 1000 synthetic posts to {TOPIC}...")
    for i in range(1000):
        post = generate_synthetic_post()
        producer.produce(TOPIC, value=json.dumps(post).encode('utf-8'))
        if i % 100 == 0:
            producer.flush()
    producer.flush()
    print("Done. Check Redis/API for updates.")

if __name__ == "__main__":
    main()
