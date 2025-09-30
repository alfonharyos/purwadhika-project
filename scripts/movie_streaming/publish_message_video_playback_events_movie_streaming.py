import os
import time
import random
import sys
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from helpers.pubsub_helper import PubSubHelper

# ==================================================================
load_dotenv("./env")
PROJECT_ID = os.getenv("BQ_PROJECT_ID")
TOPIC_ID = os.getenv("PUB_MOVIE_STREAMING_VIDEO_PLAYBACK_TOPIC")

ps = PubSubHelper(PROJECT_ID)
# ==================================================================

CITIES = [
    'Jakarta', 'Surabaya', 'Bandung', 'Medan', 'Denpasar',
    'Pekanbaru', 'Palembang', 'Padang', 'Pontianak',
    'Samarinda', 'Manado', 'Makassar'
]
ISPS = ['Indihome', 'FirstMedia', 'Biznet', 'XL', 'Telkomsel']
COUNTRY = "Indonesia"

# Buat session pool sekali aja (biar gak perlu pandas)
WATCH_SESSIONS = [
    {"session_id": i, "user_id": random.randint(1, 50), "movie_id": random.randint(1, 20)}
    for i in range(1, 101)
]

EVENT_TYPES = ['start', 'buffer', 'error', 'stop']
# Tambahkan lebih banyak error (biar proporsinya sama kayak kode awal)
WEIGHTED_EVENT_TYPES = ['start', 'buffer', 'error', 'error', 'error', 'stop']


def video_playback_events_generator(n=5):
    """Generate n random playback events"""
    now = datetime.now(ZoneInfo("Asia/Jakarta")).strftime('%Y-%m-%d %H:%M:%S')
    events = []
    for i in range(n):
        session = random.choice(WATCH_SESSIONS)
        events.append({
            "event_id": random.randint(1, 1_000_000), 
            "session_id": session["session_id"],
            "user_id": session["user_id"],
            "movie_id": session["movie_id"],
            "event_type": random.choice(WEIGHTED_EVENT_TYPES),
            "event_timestamp": now,
            "buffer_duration_ms": random.randint(0, 5000),
            "bitrate_kbps": random.randint(500, 5000),
            "city": random.choice(CITIES),
            "isp": random.choice(ISPS),
            "country": COUNTRY,
        })
    return events


def ensure_publisher():
    try:
        result = ps.create_topic(TOPIC_ID)
        print(result)
    except Exception as e:
        print(f"[ERROR] Ensure topic failed: {e}")


def publish_events():
    events = video_playback_events_generator(n=5)
    msg_ids = ps.publish_batch(TOPIC_ID, events)
    print(f"Published batch ({len(msg_ids)} events)")


if __name__ == "__main__":
    ensure_publisher()

    while True:
        publish_events()
        time.sleep(random.randint(10, 60))
