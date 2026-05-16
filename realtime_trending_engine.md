# Real-Time Social Media Trending Engine
### Project Requirements for Coding Agent

---

## Project Overview

Build a real-time streaming pipeline that ingests high-volume social media data, detects trending hashtags, named entities, and topics using sliding window aggregations, and serves a live leaderboard dashboard updated every 30 seconds.

---

## Dataset

**Primary Source: Bluesky Firehose (Recommended)**

- **Why:** Fully open, no API key required, real-time WebSocket stream, no rate limits, ~2–10 GB/day raw JSON
- **Endpoint:** `wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos`
- **Data includes:** Posts (skeets), reposts, likes, profile updates — all as CBOR-encoded CAR files decoded to JSON
- **Expected volume:** 3–8 GB/day filtered to posts only; 10–20 GB/day full firehose
- **Decoder library:** `atproto` Python SDK or `@atproto/api` Node.js SDK

**Fallback Source: Mastodon Firehose**
- **Endpoint:** `https://mastodon.social/api/v1/streaming/public` (SSE stream)
- **Volume:** ~1–3 GB/day from mastodon.social alone; aggregate multiple instances for >2 GB
- **No auth required** for public timeline streaming

> ⚠️ Dataset must exceed **2 GB** in the ingestion layer. Use Bluesky firehose without filters to guarantee this. Archive raw Kafka topics to disk/S3 to demonstrate volume.

---

## Tech Stack

| Layer | Tool |
|---|---|
| Ingestion | Apache Kafka |
| Stream Processing | Apache Flink |
| Hot Cache / Leaderboard | Redis |
| Long-term Storage | Apache Cassandra |
| Visualization | Grafana |
| Ingestion Producer | Python (`atproto` SDK) |
| Flink Jobs | Python (PyFlink) or Java |

---

## Architecture Diagram (Text)

```
Bluesky Firehose (WebSocket)
        │
        ▼
  Kafka Producer (Python)
        │
        ▼
 Kafka Topic: raw-posts
        │
        ▼
  Apache Flink Job
  ┌─────────────────────────────────┐
  │  Parse → Extract hashtags,      │
  │  entities, topics               │
  │  Sliding Window (5min/30s slide)│
  │  Count & rank per window        │
  └─────────────────────────────────┘
        │                │
        ▼                ▼
  Redis Sorted Sets   Cassandra
  (live leaderboard)  (historical archive)
        │
        ▼
     Grafana
  (live dashboard, 30s refresh)
```

---

## Phase 1 — Infrastructure Setup

### 1.1 Environment
- [ ] Docker Compose file with all services: Kafka (with Zookeeper or KRaft), Flink (JobManager + TaskManager), Redis, Cassandra, Grafana
- [ ] Minimum resource allocation: 16 GB RAM, 4 CPU cores recommended
- [ ] All services on a shared Docker network

### 1.2 Kafka Setup
- [ ] Create Kafka topic `raw-posts` with **6 partitions**, replication factor 1 (single node)
- [ ] Create Kafka topic `trending-hashtags`, `trending-entities` (output topics)
- [ ] Configure log retention: minimum **48 hours** (to accumulate >2 GB on disk)
- [ ] Enable Kafka UI (e.g., Redpanda Console or Kafka UI image) for monitoring

### 1.3 Cassandra Setup
- [ ] Keyspace: `trending_engine`
- [ ] Tables:
  ```sql
  CREATE TABLE trending_hashtags (
      window_start timestamp,
      window_end   timestamp,
      hashtag      text,
      count        bigint,
      PRIMARY KEY ((window_start), count, hashtag)
  ) WITH CLUSTERING ORDER BY (count DESC);

  CREATE TABLE trending_entities (
      window_start timestamp,
      window_end   timestamp,
      entity       text,
      entity_type  text,  -- PERSON, ORG, LOCATION, TOPIC
      count        bigint,
      PRIMARY KEY ((window_start), count, entity)
  ) WITH CLUSTERING ORDER BY (count DESC);

  CREATE TABLE raw_post_archive (
      ingested_at  timestamp,
      post_id      text,
      author_did   text,
      text         text,
      hashtags     list<text>,
      language     text,
      PRIMARY KEY (ingested_at, post_id)
  );
  ```

### 1.4 Redis Setup
- [ ] Use Redis Sorted Sets for leaderboards:
  - `leaderboard:hashtags:current` — ZSET scored by count
  - `leaderboard:entities:current` — ZSET scored by count
  - `leaderboard:topics:current` — ZSET scored by count
- [ ] Set TTL of 90 seconds on all leaderboard keys (auto-expire stale windows)
- [ ] Enable Redis persistence (AOF) for crash recovery

---

## Phase 2 — Data Ingestion (Kafka Producer)

### 2.1 Bluesky Firehose Consumer
- [ ] Connect to `wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos` using the `atproto` Python SDK
- [ ] Filter events to `app.bsky.feed.post` type only
- [ ] Decode CBOR/CAR payloads to plain Python dicts
- [ ] Extract fields: `post_id`, `author_did`, `text`, `created_at`, `langs`, `reply` (boolean), `embed` (type)
- [ ] Serialize each post as JSON and produce to Kafka topic `raw-posts`
- [ ] Implement **backpressure handling**: if Kafka producer queue is full, log and drop (do not block the WebSocket)
- [ ] Log throughput metrics every 60 seconds (messages/sec, bytes/sec)
- [ ] Producer must be fault-tolerant: auto-reconnect to firehose on disconnect with exponential backoff

### 2.2 Volume Validation
- [ ] Implement a monitoring script that checks Kafka topic size on disk every 10 minutes
- [ ] Alert (log warning) if ingestion rate drops below 500 messages/second
- [ ] The raw Kafka topic must accumulate **>2 GB** within 6 hours of running — document this in README

---

## Phase 3 — Stream Processing (Apache Flink)

### 3.1 Flink Job: Hashtag Extractor
- [ ] Consume from Kafka topic `raw-posts`
- [ ] Parse JSON, extract all hashtags from `text` field using regex: `#[a-zA-Z0-9_]+`
- [ ] Normalize: lowercase, strip punctuation
- [ ] Filter out hashtags shorter than 2 characters or longer than 50 characters
- [ ] Emit one event per hashtag per post: `{hashtag, post_id, timestamp}`

### 3.2 Flink Job: Named Entity Extractor
- [ ] Use spaCy `en_core_web_sm` model (or `xx_ent_wiki_sm` for multilingual) inside a Flink `MapFunction`
- [ ] Extract entities of types: `PERSON`, `ORG`, `GPE` (location), `EVENT`, `PRODUCT`
- [ ] Emit: `{entity_text, entity_type, post_id, timestamp}`
- [ ] **Performance note:** Initialize spaCy model once per TaskManager slot using `open()` method, not per-record

### 3.3 Flink Job: Topic Classifier
- [ ] Use keyword-based topic bucketing (no heavy ML needed):
  - Politics, Sports, Technology, Entertainment, Finance, Health, Climate
- [ ] Classify each post into 1–3 topics based on keyword matching (provide keyword lists in config)
- [ ] Emit: `{topic, post_id, timestamp}`

### 3.4 Sliding Window Aggregations
- [ ] Apply **sliding window** on all three streams:
  - Window size: **5 minutes**
  - Slide interval: **30 seconds**
- [ ] For each window, compute **Top 50** by count for hashtags, entities, and topics
- [ ] Use Flink's `SlidingEventTimeWindows` with watermark strategy (max out-of-orderness: 10 seconds)
- [ ] Output schema per window result:
  ```json
  {
    "window_start": "2025-01-01T12:00:00Z",
    "window_end":   "2025-01-01T12:05:00Z",
    "type":         "hashtag",
    "term":         "bluesky",
    "count":        1423
  }
  ```

### 3.5 Flink Sinks
- [ ] **Redis Sink:** On each window trigger, replace sorted set `leaderboard:{type}:current` with new top-50 using a pipeline of `ZADD` commands; set TTL 90s
- [ ] **Cassandra Sink:** Write all window results to respective Cassandra tables for historical analysis
- [ ] **Kafka Sink:** Publish window results to `trending-hashtags` and `trending-entities` topics for downstream consumers

---

## Phase 4 — Leaderboard API (Optional but Recommended)

### 4.1 REST API
- [ ] Lightweight FastAPI or Flask service
- [ ] Endpoints:
  - `GET /leaderboard/hashtags?limit=20` — reads from Redis `leaderboard:hashtags:current`
  - `GET /leaderboard/entities?limit=20` — reads from Redis
  - `GET /leaderboard/topics` — reads from Redis
  - `GET /history/hashtags?from=<ts>&to=<ts>` — queries Cassandra
- [ ] Response format:
  ```json
  {
    "updated_at": "2025-01-01T12:05:30Z",
    "window": "5min",
    "results": [
      {"rank": 1, "term": "#bluesky", "count": 1423},
      {"rank": 2, "term": "#fediverse", "count": 987}
    ]
  }
  ```
- [ ] Add CORS headers for Grafana JSON datasource compatibility

---

## Phase 5 — Visualization (Grafana)

### 5.1 Grafana Setup
- [ ] Use **Redis datasource plugin** (`redis-datasource`) for live leaderboard panels
- [ ] Use **Cassandra datasource plugin** or JSON API datasource for historical panels

### 5.2 Dashboard Panels (single dashboard: "Trending Engine")

| Panel | Type | Source | Refresh |
|---|---|---|---|
| Top 20 Hashtags (now) | Bar gauge | Redis ZREVRANGE | 30s |
| Top 20 Entities (now) | Bar gauge | Redis ZREVRANGE | 30s |
| Topic Distribution | Pie chart | Redis | 30s |
| Hashtag Trend Over Time | Time series | Cassandra | 1min |
| Ingestion Rate (msg/sec) | Stat | Kafka metrics | 30s |
| Flink Job Lag | Stat | Flink REST API | 30s |

- [ ] Set dashboard auto-refresh to **30 seconds**
- [ ] Add a template variable `$window_size` (5min, 10min, 1hr) for historical panels
- [ ] Export dashboard as `grafana_dashboard.json` and include in repo

---

## Phase 6 — Testing & Validation

### 6.1 Unit Tests
- [ ] Test hashtag regex extractor with edge cases (URLs, email addresses, non-ASCII)
- [ ] Test NER extraction on sample posts
- [ ] Test topic classifier keyword matching
- [ ] Test Redis sink (mock Redis, verify ZADD commands)

### 6.2 Integration Tests
- [ ] End-to-end test: produce 10,000 synthetic posts to Kafka → verify leaderboard updates in Redis within 60 seconds
- [ ] Test window correctness: inject posts with known hashtag frequencies, assert Top-1 result

### 6.3 Load / Volume Test
- [ ] Run ingestion for minimum **2 hours** and document:
  - Total GB ingested to Kafka
  - Flink consumer lag (must stay < 30 seconds)
  - Redis leaderboard update latency (p99 < 5 seconds after window close)
  - Cassandra write throughput

---

## Phase 7 — Documentation & Deployment

### 7.1 Repository Structure
```
/
├── docker-compose.yml
├── README.md
├── producer/
│   ├── bluesky_producer.py
│   ├── requirements.txt
│   └── Dockerfile
├── flink_jobs/
│   ├── hashtag_extractor.py
│   ├── entity_extractor.py
│   ├── topic_classifier.py
│   ├── sliding_window_agg.py
│   └── requirements.txt
├── api/
│   ├── main.py
│   └── requirements.txt
├── cassandra/
│   └── schema.cql
├── grafana/
│   └── dashboard.json
├── config/
│   ├── kafka_config.yaml
│   ├── flink_config.yaml
│   └── topic_keywords.yaml
└── tests/
    ├── test_extractors.py
    └── test_integration.py
```

### 7.2 README Must Include
- [ ] Architecture diagram (ASCII or image)
- [ ] Prerequisites and setup steps
- [ ] `docker compose up` one-command startup
- [ ] Screenshot of Grafana dashboard
- [ ] Evidence of >2 GB dataset (Kafka topic size screenshot or log)
- [ ] Performance metrics from load test

### 7.3 Config File: `topic_keywords.yaml`
```yaml
topics:
  technology: [ai, llm, programming, software, github, python, cloud, api, blockchain]
  politics:   [election, government, president, senate, vote, policy, congress]
  sports:     [nfl, nba, soccer, football, cricket, tennis, worldcup, olympics]
  finance:    [bitcoin, stock, market, crypto, trading, inflation, fed, nasdaq]
  health:     [vaccine, covid, cancer, mental health, fda, hospital, nutrition]
  climate:    [climate, wildfire, flood, solar, renewable, emissions, ipcc]
  entertainment: [movie, music, netflix, spotify, album, concert, grammy, oscar]
```

---

## Acceptance Criteria (Definition of Done)

| # | Criterion |
|---|---|
| 1 | Bluesky firehose producer runs continuously and produces >500 msg/sec to Kafka |
| 2 | Kafka `raw-posts` topic accumulates >2 GB of data within 6 hours |
| 3 | Flink sliding window (5min window, 30s slide) processes all three stream types |
| 4 | Redis leaderboard updates every 30 seconds with correct Top-20 rankings |
| 5 | Cassandra contains historical window data queryable by time range |
| 6 | Grafana dashboard auto-refreshes every 30 seconds and shows live data |
| 7 | End-to-end latency from post creation to leaderboard update < 60 seconds |
| 8 | System runs stably for 2+ hours without manual intervention |
| 9 | All unit and integration tests pass |
| 10 | README documents setup, architecture, and evidence of >2 GB dataset |

---

## Key Libraries & Versions

```txt
# Producer
atproto==0.0.54
confluent-kafka==2.3.0
websockets==12.0

# Flink Jobs
apache-flink==1.18.0
spacy==3.7.2
redis==5.0.1
cassandra-driver==3.29.0

# API
fastapi==0.110.0
uvicorn==0.27.0

# Testing
pytest==8.0.0
pytest-asyncio==0.23.0
fakeredis==2.21.0
``

---