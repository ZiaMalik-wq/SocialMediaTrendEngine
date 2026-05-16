import os
import logging
from datetime import datetime, timezone
from pyflink.datastream.functions import RuntimeContext
from pyflink.datastream import MapFunction
import redis
from cassandra.cluster import Cluster

logger = logging.getLogger(__name__)

class RedisLeaderboardSink(MapFunction):
    def __init__(self, leaderboard_type: str):
        self.leaderboard_type = leaderboard_type
        self.redis_host = os.getenv("REDIS_HOST", "redis")
        self.redis_port = int(os.getenv("REDIS_PORT", 6379))
        self.r = None

    def open(self, runtime_context: RuntimeContext):
        self.r = redis.Redis(
            host=self.redis_host,
            port=self.redis_port,
            decode_responses=True,
            socket_connect_timeout=5,
        )
        self.r.ping()
        logger.info(f"[RedisLeaderboardSink:{self.leaderboard_type}] connected")

    def map(self, value):
        try:
            term, count = value
            key = f"leaderboard:{self.leaderboard_type}:current"
            hash_key = f"{key}:hash"
            pipe = self.r.pipeline()

            # Add/update this term's score
            pipe.zadd(key, {term: float(count)})

            # Keep top 50, but do it safely
            pipe.zremrangebyrank(key, 0, -52) 

            pipe.expire(key, 600)
            pipe.set("leaderboard:last_update", int(datetime.now(timezone.utc).timestamp() * 1000))
            pipe.execute()

            top_items = self.r.zrevrange(key, 0, 49, withscores=True)
            pipe = self.r.pipeline()
            pipe.delete(hash_key)
            if top_items:
                pipe.hset(hash_key, mapping={item: float(score) for item, score in top_items})
            pipe.expire(hash_key, 600)
            pipe.execute()
        except Exception as e:
            logger.error(f"[RedisLeaderboardSink] error: {e}")
        return value

    def close(self):
        if self.r:
            self.r.close()

class CassandraWindowSink(MapFunction):
    def __init__(self, table_name: str, column_name: str):
        self.table_name     = table_name
        self.column_name    = column_name
        self.cassandra_host = os.getenv("CASSANDRA_HOST", "cassandra")
        self.cluster        = None
        self.session        = None
        self.prep_stmt      = None

    def open(self, runtime_context: RuntimeContext):
        self.cluster = Cluster([self.cassandra_host], connect_timeout=10)
        self.session = self.cluster.connect('trending_engine')
        self.prep_stmt = self.session.prepare(
            f"INSERT INTO {self.table_name} "
            f"(window_start, window_end, {self.column_name}, count) VALUES (?, ?, ?, ?)"
        )
        logger.info(f"[CassandraWindowSink] connected -> {self.table_name}")

    def map(self, value):
        try:
            w_start_str, w_end_str, term, count = value
            
            # CRITICAL: Convert strings back to datetime objects for Cassandra
            w_start = datetime.fromisoformat(w_start_str.replace('Z', '+00:00'))
            w_end   = datetime.fromisoformat(w_end_str.replace('Z', '+00:00'))
            
            self.session.execute(self.prep_stmt, (w_start, w_end, term, int(count)))
        except Exception as e:
            logger.error(f"[CassandraWindowSink] error: {e}")
        return value

    def close(self):
        if self.cluster:
            try:
                self.cluster.shutdown()
            except Exception:
                pass