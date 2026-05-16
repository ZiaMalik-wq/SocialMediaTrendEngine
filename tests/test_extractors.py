import pytest
import json
from flink_jobs.sliding_window_agg import HashtagExtractor, TopicClassifier

def test_hashtag_extractor():
    extractor = HashtagExtractor()
    post = json.dumps({
        "text": "Hello world! #bluesky #streaming #data_engine",
        "ingested_at": "2024-01-01T12:00:00Z"
    })
    results = extractor.map(post)
    hashtags = [r[0] for r in results]
    assert "bluesky" in hashtags
    assert "streaming" in hashtags
    assert "data_engine" in hashtags
    assert len(hashtags) == 3

def test_topic_classifier():
    classifier = TopicClassifier()
    # Mock topics since we're testing the logic
    classifier.topics = {
        "technology": ["python", "ai"],
        "sports": ["nba", "soccer"]
    }
    
    post = json.dumps({
        "text": "I love coding in python and watching nba games",
        "ingested_at": "2024-01-01T12:00:00Z"
    })
    results = classifier.map(post)
    topics = [r[0] for r in results]
    assert "technology" in topics
    assert "sports" in topics
    assert len(topics) == 2
