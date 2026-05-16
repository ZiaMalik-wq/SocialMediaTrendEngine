CREATE KEYSPACE IF NOT EXISTS trending_engine
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

USE trending_engine;

CREATE TABLE IF NOT EXISTS trending_hashtags (
    window_start timestamp,
    window_end   timestamp,
    hashtag      text,
    count        bigint,
    PRIMARY KEY ((window_start), count, hashtag)
) WITH CLUSTERING ORDER BY (count DESC, hashtag ASC);

CREATE TABLE IF NOT EXISTS trending_entities (
    window_start timestamp,
    window_end   timestamp,
    entity       text,
    entity_type  text,  -- PERSON, ORG, LOCATION, TOPIC
    count        bigint,
    PRIMARY KEY ((window_start), count, entity)
) WITH CLUSTERING ORDER BY (count DESC, entity ASC);

CREATE TABLE IF NOT EXISTS trending_topics (
    window_start timestamp,
    window_end   timestamp,
    topic        text,
    count        bigint,
    PRIMARY KEY ((window_start), count, topic)
) WITH CLUSTERING ORDER BY (count DESC, topic ASC);

CREATE TABLE IF NOT EXISTS raw_post_archive (
    ingested_at  timestamp,
    post_id      text,
    author_did   text,
    text         text,
    hashtags     list<text>,
    language     text,
    PRIMARY KEY (ingested_at, post_id)
);
