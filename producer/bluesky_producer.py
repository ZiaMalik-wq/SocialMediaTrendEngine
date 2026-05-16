import os
import json
import time
import logging
import asyncio
from datetime import datetime
from atproto import FirehoseSubscribeReposClient, models, parse_subscribe_repos_message, CAR
from confluent_kafka import Producer

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "raw-posts")
METRICS_INTERVAL_SEC    = 60
RECONNECT_DELAY_SEC     = 5

# ─── Kafka Producer ───────────────────────────────────────────────────────────
producer = Producer({
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'client.id':         'bluesky-producer',
    'linger.ms':         100,
    'compression.type':  'snappy',
    'queue.buffering.max.messages': 100_000,
    'queue.buffering.max.kbytes':   256_000,
})

# ─── Metrics ──────────────────────────────────────────────────────────────────
msg_count       = 0
byte_count      = 0
drop_count      = 0
last_metric_ts  = time.time()


def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Kafka delivery failed: {err}")


async def log_metrics():
    """Log throughput every METRICS_INTERVAL_SEC seconds."""
    global msg_count, byte_count, drop_count, last_metric_ts

    while True:
        await asyncio.sleep(METRICS_INTERVAL_SEC)
        now      = time.time()
        elapsed  = now - last_metric_ts or 1
        mps      = msg_count  / elapsed
        kbps     = byte_count / elapsed / 1024
        logger.info(
            f"Throughput: {mps:.1f} msg/sec | {kbps:.1f} KB/sec | "
            f"drops: {drop_count} | total sent: {msg_count}"
        )
        msg_count      = 0
        byte_count     = 0
        drop_count     = 0
        last_metric_ts = now


# ─── Record Parsing ───────────────────────────────────────────────────────────
def extract_post(op, repo_did: str, record) -> dict | None:
    """
    Extract a flat, JSON-serialisable dict from a raw atproto record.
    Returns None if the record is invalid or not a text post.
    """
    try:
        if not isinstance(record, dict):
            return None

        text = record.get('text', '')
        if not isinstance(text, str):
            return None

        embed      = record.get('embed') or {}
        embed_type = embed.get('$type') if isinstance(embed, dict) else None

        return {
            "post_id":    op.path.split('/')[-1],
            "author_did": repo_did,
            "text":       text,
            "created_at": record.get('created_at', datetime.utcnow().isoformat()),
            "langs":      record.get('langs') or [],
            "reply":      bool(record.get('reply')),
            "embed_type": embed_type,
            "ingested_at": datetime.utcnow().isoformat(),
        }
    except Exception:
        return None


# ─── Firehose Message Handler ─────────────────────────────────────────────────
def on_message_handler(message):
    """
    Called for every raw frame from the Bluesky firehose.

    Frame types include #commit, #sync, #info, #handle, #tombstone, etc.
    Only #commit frames carry post data; all others are silently ignored.
    """
    global msg_count, byte_count, drop_count

    # ── Step 1: parse the outer frame (may raise for non-commit frame types) ──
    try:
        commit = parse_subscribe_repos_message(message)
    except Exception:
        # Non-commit control frames (#sync, #info, #handle …) raise here.
        # This is expected behaviour — silently discard.
        return

    # ── Step 2: type-guard — only process Commit frames ───────────────────────
    if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
        return

    if not commit.blocks:
        return

    # ── Step 3: decode CAR blocks and iterate ops ─────────────────────────────
    try:
        car = CAR.from_bytes(commit.blocks)

        for op in commit.ops:
            if op.action != 'create':
                continue
            if not op.path.startswith('app.bsky.feed.post'):
                continue

            record = car.blocks.get(op.cid)
            if record is None:
                continue

            post = extract_post(op, commit.repo, record)
            if post is None:
                continue

            payload = json.dumps(post, ensure_ascii=False).encode('utf-8')

            try:
                producer.produce(
                    KAFKA_TOPIC,
                    value=payload,
                    on_delivery=delivery_report,
                )
                msg_count  += 1
                byte_count += len(payload)
                # Non-blocking poll to serve delivery callbacks
                producer.poll(0)

            except BufferError:
                drop_count += 1
                # Don't log every drop — metrics will show the count

    except Exception as e:
        logger.error(f"Error processing commit {getattr(commit, 'seq', '?')}: {e}")


# ─── Firehose Runner ──────────────────────────────────────────────────────────
async def run_firehose():
    """
    Connect to the Bluesky firehose and stream indefinitely.
    Reconnects automatically on any connection error.
    """
    client = FirehoseSubscribeReposClient()
    loop   = asyncio.get_event_loop()

    while True:
        try:
            logger.info("Connecting to Bluesky firehose...")
            # client.start() is blocking — run in a thread pool executor
            await loop.run_in_executor(None, client.start, on_message_handler)
        except Exception as e:
            logger.error(
                f"Firehose connection lost: {e}. "
                f"Reconnecting in {RECONNECT_DELAY_SEC}s..."
            )
            await asyncio.sleep(RECONNECT_DELAY_SEC)


# ─── Entry Point ──────────────────────────────────────────────────────────────
async def main():
    logger.info(f"Starting Bluesky producer → Kafka [{KAFKA_BOOTSTRAP_SERVERS}] topic [{KAFKA_TOPIC}]")
    asyncio.create_task(log_metrics())
    await run_firehose()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down — flushing Kafka producer...")
        producer.flush(timeout=10)
        logger.info("Done.")