import os
import redis
from fastapi import FastAPI, Query
from typing import List, Dict
from datetime import datetime
from cassandra.cluster import Cluster

app = FastAPI(title="Trending Engine Leaderboard API")

# Redis Connection
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# Cassandra Connection
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
cluster = None
session = None

def get_cassandra():
    global cluster, session
    if not session:
        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect('trending_engine')
    return session

@app.get("/leaderboard/hashtags")
def get_hashtags(limit: int = 20):
    data = r.zrevrange("leaderboard:hashtags:current", 0, limit - 1, withscores=True)
    results = [{"rank": i + 1, "term": term, "count": int(score)} for i, (term, score) in enumerate(data)]
    return {
        "updated_at": datetime.utcnow().isoformat(),
        "window": "5min",
        "results": results
    }

@app.get("/leaderboard/entities")
def get_entities(limit: int = 20):
    data = r.zrevrange("leaderboard:entities:current", 0, limit - 1, withscores=True)
    results = [{"rank": i + 1, "term": term, "count": int(score)} for i, (term, score) in enumerate(data)]
    return {
        "updated_at": datetime.utcnow().isoformat(),
        "window": "5min",
        "results": results
    }

@app.get("/leaderboard/topics")
def get_topics():
    data = r.zrevrange("leaderboard:topics:current", 0, -1, withscores=True)
    results = [{"rank": i + 1, "term": term, "count": int(score)} for i, (term, score) in enumerate(data)]
    return {
        "updated_at": datetime.utcnow().isoformat(),
        "window": "5min",
        "results": results
    }

@app.get("/health")
def health():
    return {"status": "ok"}
