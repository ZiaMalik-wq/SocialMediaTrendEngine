# Real-Time Social Media Trending Engine

A high-performance streaming pipeline that ingests Bluesky firehose data, detects trending hashtags, named entities, and topics using sliding window aggregations, and serves a live leaderboard.

## Architecture

```
Bluesky Firehose (WebSocket)
        │
        ▼
  Kafka Producer (Python) ──▶ Kafka Topic: raw-posts
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
     Grafana / FastAPI
  (live dashboard & API)
```

## Tech Stack

- **Ingestion**: Python (atproto SDK), Apache Kafka
- **Stream Processing**: Apache Flink (PyFlink)
- **Hot Cache**: Redis (Sorted Sets)
- **Historical Storage**: Apache Cassandra
- **API**: FastAPI
- **Visualization**: Grafana

## Setup & Running

### Prerequisites

- **Docker & Docker Compose** (latest stable)
- **16GB RAM** minimum recommended
- **Windows/Linux/macOS** (tested on Windows 10+, WSL2)
- Port availability: 8080 (Kafka UI), 8081 (Flink), 8000 (API), 3001 (Grafana), 6379 (Redis), 9042 (Cassandra), 9092 (Kafka)

### Quick Start (Single Command)

```powershell
# Clone and navigate to project
cd SocialMediaTrendEngine

# Start all services in one go
docker compose up -d

# Wait 30-45 seconds for all services to initialize, then check status
docker compose ps
```

### Verify Services Are Running

```powershell
# Check all containers are healthy
docker compose ps

# Expected output:
# NAME              STATUS          PORTS
# kafka             Up (healthy)    0.0.0.0:9092->9092/tcp, 0.0.0.0:29092->29092/tcp
# redis             Up (healthy)    0.0.0.0:6379->6379/tcp
# cassandra         Up (healthy)    0.0.0.0:9042->9042/tcp
# jobmanager        Up (healthy)    0.0.0.0:8081->8081/tcp
# taskmanager       Up              (no ports)
# grafana           Up              0.0.0.0:3001->3000/tcp
# flink-job-submit  Up
# bluesky-producer  Up
# trending-api      Up              0.0.0.0:8000->8000/tcp
```

### Access the Services

| Service               | URL                                                      | Credentials      | Purpose                                                |
| --------------------- | -------------------------------------------------------- | ---------------- | ------------------------------------------------------ |
| **Grafana Dashboard** | [http://localhost:3001](http://localhost:3001)           | Admin / admin123 | Live trending leaderboard (hashtags, entities, topics) |
| **Flink Dashboard**   | [http://localhost:8081](http://localhost:8081)           | —                | Monitor stream processing jobs & task managers         |
| **Kafka UI**          | [http://localhost:8080](http://localhost:8080)           | —                | Monitor Kafka topics & message production              |
| **API (Swagger)**     | [http://localhost:8000/docs](http://localhost:8000/docs) | —                | Query trending data programmatically                   |
| **Redis CLI**         | Command-line only                                        | —                | Direct Redis inspection                                |
| **Cassandra**         | Command-line only                                        | —                | Historical data archive                                |

### Features

- **Real-time Ingestion**: Connects directly to Bluesky's network firehose via WebSocket.
- **Live-Only Processing**: Kafka offset set to `latest()` with stale-record filtering (default 30 min cutoff).
- **NER Extraction**: Uses spaCy for named entity recognition (Person, Org, Location, Event, Product).
- **Topic Classification**: Keyword-based bucketing for domains like Tech, Politics, Sports (configurable).
- **Sliding Windows**: 1-minute aggregation windows with 10-second sliding interval (tunable in `config/flink_config.yaml`).
- **Dual Output**:
  - **Redis**: Live leaderboard (updated every ~10 seconds, expires after 10 minutes)
  - **Cassandra**: Permanent historical archive for trend analysis
- **Grafana Formatting**: Relative time display ("10 seconds ago") for last-update timestamp.
- **Fault Tolerant**: Kafka log retention (48 hours) and Redis persistence enabled.

## Configuration

### Environment Variables

```powershell
# For stale-record filtering (default: 30 minutes)
$env:MAX_RECORD_AGE_MINUTES = 30

# Kafka group ID (default: flink-aggregator-live)
$env:KAFKA_GROUP_ID = "flink-aggregator-live"

# To override, pass to Flink job:
docker compose down flink-job-submit
$env:MAX_RECORD_AGE_MINUTES = 5
docker compose up -d flink-job-submit
```

### File Configuration

- **Topic keywords**: `config/topic_keywords.yaml` — define which keywords trigger topic classification.
- **Flink tuning**: `config/flink_config.yaml` — window size, slide interval, checkpoint settings.
- **Kafka settings**: `docker-compose.yml` under the `kafka` service.

## Operational Commands

### Monitor Live Data

**Check Redis leaderboard:**

```powershell
# Top 10 hashtags
docker compose exec redis redis-cli zrevrange leaderboard:hashtags:current 0 9 withscores

# Last update timestamp (epoch ms, e.g., "1778931226467")
docker compose exec redis redis-cli get leaderboard:last_update

# Check data TTL (how long until expiry)
docker compose exec redis redis-cli ttl leaderboard:hashtags:current:hash
```

**View Flink job logs:**

```powershell
docker compose logs flink-job-submit -f  # Follow in real-time
docker compose logs jobmanager --tail 20  # Last 20 lines
```

**Check Cassandra historical data:**

```powershell
# Count rows in trending_hashtags table
docker compose exec cassandra cqlsh cassandra \
  -e "SELECT COUNT(*) FROM trending_engine.trending_hashtags;" \
  -k trending_engine

# Query recent windows
docker compose exec cassandra cqlsh cassandra \
  -e "SELECT window_start, hashtag, count FROM trending_engine.trending_hashtags LIMIT 10;" \
  -k trending_engine
```

### Restart Components

**Full restart (start fresh):**

```powershell
docker compose down
docker compose up -d
Start-Sleep -Seconds 45  # Wait for initialization
```

**Restart only Grafana (if dashboard not loading):**

```powershell
docker compose restart grafana
Start-Sleep -Seconds 5
Start-Process http://localhost:3001
```

**Restart Flink job (picks up code changes):**

```powershell
docker compose down flink-job-submit jobmanager taskmanager
docker compose up -d jobmanager taskmanager flink-job-submit
```

**Clear live Redis data (start fresh leaderboard):**

```powershell
docker compose exec redis redis-cli FLUSHDB
```

### Rebuild Images (After Code Changes)

```powershell
# Rebuild Flink job (sliding_window_agg.py, filters.py, entity_extractor.py)
docker compose up -d --build jobmanager taskmanager flink-job-submit

# Rebuild Producer or API
docker compose up -d --build producer
docker compose up -d --build api
```

## Troubleshooting

### Grafana Dashboard Shows No Data

1. **Check Grafana health**: `curl http://localhost:3001/api/health`
2. **Hard refresh browser**: `Ctrl+Shift+R`
3. **Verify Redis connection**: `docker compose exec redis redis-cli ping` (should return `PONG`)
4. **Check Flink job status**: Visit [http://localhost:8081](http://localhost:8081) → Job name → Task Managers
5. **Restart Grafana**: `docker compose restart grafana && sleep 5`

### Stale/Old Hashtags Still Showing

- **Cause**: Kafka still has old messages, Flink is processing them.
- **Solution**: Either:
  - Reduce `MAX_RECORD_AGE_MINUTES` to filter more aggressively
  - Clear Redis: `docker compose exec redis redis-cli FLUSHDB`
  - Wait 30+ minutes for old messages to naturally age out

### Kafka Topic Errors

- Kafka auto-creates topics when first message arrives (set in `docker-compose.yml`).
- To view topics: `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server kafka:9092`

### Out of Memory or Slow Response

- Increase Docker memory allocation to container or host.
- Reduce Flink parallelism in `docker-compose.yml`: `parallelism.default: 1`

## Recent Changes (May 2026)

### Kafka Offset & Live-Only Behavior

- Switched Flink Kafka source from `earliest()` to `latest()` to consume only new messages
- Changed consumer group ID to `flink-aggregator-live` to avoid replaying old committed offsets
- **Effect**: No more historical hashtags polluting the live leaderboard

### Stale-Record Filtering

- Added `flink_jobs/filters.py` with `is_recent_record()` check
- Records with `ingested_at` older than 30 minutes are dropped before aggregation
- **Configurable via**: `MAX_RECORD_AGE_MINUTES` environment variable

### Grafana Timestamp Formatting

- Last-update marker now stored as epoch milliseconds (instead of ISO string)
- Grafana stat panel configured with `unit: "dateTimeFromNow"` for relative time display
- **Display**: "10 seconds ago" instead of raw timestamp

### Files Modified

- `flink_jobs/sliding_window_agg.py` — Kafka offset, group ID, stale filter integration
- `flink_jobs/entity_extractor.py` — Stale record filtering
- `flink_jobs/utils.py` — Epoch millisecond timestamp storage
- `flink_jobs/filters.py` — New shared filtering module
- `grafana/dashboard.json` — Timestamp unit formatting

## Architecture Overview

**Data Flow:**

```
Bluesky Firehose → Kafka (raw-posts)
                       ↓
                   Flink Job
                   ├─ Stale filter (>30 min dropped)
                   ├─ Extract: hashtags, entities, topics
                   ├─ Slide window: 1 min / 10 sec
                   ├─ Count & rank
                   │
                   ├─→ Redis Sorted Sets (live, TTL 10min)
                   └─→ Cassandra (historical, forever)
                       ↓
                   Grafana / FastAPI (visualization & API)
```

**Processing Latency:**

- Ingestion to Kafka: ~100ms
- Kafka to Flink: ~500ms
- Flink aggregation: 1 window size (1 min) + 1 slide (10 sec) = ~70 sec max
- Flink to Redis: <100ms
- **Total end-to-end**: ~70-90 seconds from post to leaderboard

## License

MIT
