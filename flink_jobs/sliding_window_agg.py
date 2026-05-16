import os
import json
import logging
import yaml
import re
from datetime import datetime
from typing import Iterator

from pyflink.common import WatermarkStrategy, Time, Duration
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, ProcessWindowFunction
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaRecordSerializationSchema, KafkaOffsetsInitializer
)
from pyflink.datastream.window import SlidingEventTimeWindows
from pyflink.datastream.functions import FlatMapFunction, MapFunction, RuntimeContext

from entity_extractor import EntityExtractor
from filters import is_recent_record
from utils import RedisLeaderboardSink, CassandraWindowSink

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "raw-posts")
KAFKA_GROUP_ID          = os.getenv("KAFKA_GROUP_ID", "flink-aggregator-live")
TOPIC_KEYWORDS_FILE     = "/opt/flink/config/topic_keywords.yaml"


# ─── Timestamp Assigner ───────────────────────────────────────────────────────

class IngestedAtTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        try:
            data = json.loads(value)
            dt = datetime.fromisoformat(data['ingested_at'].replace('Z', '+00:00'))
            return int(dt.timestamp() * 1000)
        except Exception:
            return record_timestamp


# ─── Hashtag Extractor ────────────────────────────────────────────────────────
# Must extend FlatMapFunction and implement flat_map() — NOT map()

class HashtagExtractor(FlatMapFunction):
    def flat_map(self, value) -> Iterator:
        try:
            data    = json.loads(value)
            if not is_recent_record(data):
                return
            text    = data.get('text', '')
            ts      = data.get('ingested_at', datetime.utcnow().isoformat())
            hashtags = re.findall(r'#([a-zA-Z0-9_]{2,50})', text)
            for h in hashtags:
                yield (h.lower(), ts)
        except Exception:
            pass  # silently drop malformed records


# ─── Topic Classifier ─────────────────────────────────────────────────────────

class TopicClassifier(FlatMapFunction):
    def open(self, runtime_context: RuntimeContext):
        path = TOPIC_KEYWORDS_FILE if os.path.exists(TOPIC_KEYWORDS_FILE) \
               else "config/topic_keywords.yaml"
        with open(path, 'r') as f:
            self.topics = yaml.safe_load(f).get('topics', {})

    def flat_map(self, value) -> Iterator:
        try:
            data = json.loads(value)
            if not is_recent_record(data):
                return
            text = data.get('text', '').lower()
            ts   = data.get('ingested_at', datetime.utcnow().isoformat())
            for topic, keywords in self.topics.items():
                if any(k in text for k in keywords):
                    yield (topic, ts)
        except Exception:
            pass


# ─── Window Result Processor ──────────────────────────────────────────────────

class WindowResultProcessor(ProcessWindowFunction):
    def process(self, key, context, elements):
        count = 0
        for e in elements:
            count = e[1]   # after reduce, one element with accumulated count
        window = context.window()
        yield (
            datetime.fromtimestamp(window.start / 1000).isoformat(),
            datetime.fromtimestamp(window.end   / 1000).isoformat(),
            key,
            count,
        )


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)
    env.set_python_executable("/usr/bin/python3")
    
    # Register local modules so TaskManagers can find them
    env.add_python_file("/opt/flink/usrlib/entity_extractor.py")
    env.add_python_file("/opt/flink/usrlib/filters.py")
    env.add_python_file("/opt/flink/usrlib/utils.py")

    # ── Kafka source ──────────────────────────────────────────────────────────
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        .set_topics(KAFKA_TOPIC)
        .set_group_id(KAFKA_GROUP_ID)
        .set_value_only_deserializer(SimpleStringSchema())
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .build()
    )

    raw_stream = env.from_source(
        source,
        WatermarkStrategy
            .for_bounded_out_of_orderness(Duration.of_seconds(10))
            .with_timestamp_assigner(IngestedAtTimestampAssigner())
            .with_idleness(Duration.of_seconds(10)),
        "Kafka Source",
    )

    # ── Load window config ────────────────────────────────────────────────────
    conf_path = (
        "/opt/flink/config/flink_config.yaml"
        if os.path.exists("/opt/flink/config/flink_config.yaml")
        else "config/flink_config.yaml"
    )
    with open(conf_path, 'r') as f:
        f_conf = yaml.safe_load(f)

    win_size  = f_conf.get('window_size_min', 5)
    win_slide = f_conf.get('window_slide_sec', 30)

    # ── Feature streams ───────────────────────────────────────────────────────
    STRING_PAIR = Types.TUPLE([Types.STRING(), Types.STRING()])

    hashtag_stream = raw_stream.flat_map(HashtagExtractor(),  output_type=STRING_PAIR)
    topic_stream   = raw_stream.flat_map(TopicClassifier(),   output_type=STRING_PAIR)

    # EntityExtractor must also extend FlatMapFunction (see entity_extractor.py)
    entity_stream  = raw_stream.flat_map(EntityExtractor(),
                                         output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.STRING()]))

    # entity_stream yields (entity_text, entity_type, ingested_at)
    # Collapse to (entity_text, ingested_at) for the same pipeline
    entity_pair_stream = entity_stream.map(
        lambda x: (x[0], x[2]),
        output_type=STRING_PAIR,
    )

    # ── Shared aggregation pipeline ───────────────────────────────────────────
    WINDOW_RESULT_TYPE = Types.TUPLE([Types.STRING(), Types.STRING(), Types.STRING(), Types.LONG()])

    def apply_pipeline(stream, label: str, cassandra_table: str, cassandra_col: str, kafka_out_topic: str = None):
        COUNT_TYPE         = Types.TUPLE([Types.STRING(), Types.INT()])
        WINDOW_RESULT_TYPE = Types.TUPLE([Types.STRING(), Types.STRING(), Types.STRING(), Types.LONG()])

        # Step 1: count-by-key inside sliding window
        counted = (
            stream
            .map(lambda x: (x[0], 1), output_type=COUNT_TYPE)
            .key_by(lambda x: x[0])
            .window(SlidingEventTimeWindows.of(Time.minutes(win_size), Time.seconds(win_slide)))
            .reduce(lambda a, b: (a[0], a[1] + b[1]))
        )

        # Step 2: Redis — use .map() as side-effect (NOT .add_sink())
        counted.map(RedisLeaderboardSink(label), output_type=COUNT_TYPE)

        # Step 3: enrich with window timestamps
        processed = (
            counted
            .key_by(lambda x: x[0])
            .window(SlidingEventTimeWindows.of(Time.minutes(win_size), Time.seconds(win_slide)))
            .process(WindowResultProcessor(), output_type=WINDOW_RESULT_TYPE)
        )

        # Step 4: Cassandra — use .map() as side-effect (NOT .add_sink())
        processed.map(CassandraWindowSink(cassandra_table, cassandra_col), output_type=WINDOW_RESULT_TYPE)

        # Step 5: optional downstream Kafka topic
        if kafka_out_topic:
            kafka_sink = (
                KafkaSink.builder()
                .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
                .set_record_serializer(
                    KafkaRecordSerializationSchema.builder()
                    .set_topic(kafka_out_topic)
                    .set_value_serialization_schema(SimpleStringSchema())
                    .build()
                )
                .build()
            )
            (
                processed
                .map(
                    lambda x: json.dumps({
                        "start": x[0], "end": x[1], "term": x[2], "count": x[3]
                    }),
                    output_type=Types.STRING(),
                )
                .sink_to(kafka_sink)
            )

    apply_pipeline(hashtag_stream,      "hashtags", "trending_hashtags", "hashtag", "trending-hashtags")
    apply_pipeline(topic_stream,        "topics",   "trending_topics",   "topic")
    apply_pipeline(entity_pair_stream,  "entities", "trending_entities", "entity", "trending-entities")

    env.execute("Social Media Trending Engine")


if __name__ == "__main__":
    main()