import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta
from helpers.postgresql_helper import get_last_id
from zoneinfo import ZoneInfo

fake = Faker()

# ---------- CONFIG ----------
GENRES = ['Action', 'Comedy', 'Drama', 'Sci-Fi', 'Thriller', 'Horror', 'Romance', 'Animation']
LANGUAGES = ['English', 'Spanish', 'French', 'German', 'Japanese', 'Korean', 'Indonesian']
PLANS = [('Basic', 50000), ('Standard', 100000), ('Premium', 150000)]
PAYMENT_METHODS = ['credit_card', 'gopay', 'dana', 'bank_transfer']
PAYMENT_STATUS = ['pending', 'completed', 'failed']
SUB_STATUS_USER = ['active', 'inactive']
SUB_STATUS = ['active', 'expired']
DEVICE_TYPES = ['mobile', 'smart_tv', 'desktop', 'tablet']
CITIES = ['Jakarta', 'Surabaya', 'Bandung', 'Medan', 'Denpasar', 'Pekanbaru', 'Palembang', 'Padang', 'Pontianak', 'Samarinda', 'Manado', 'Makassar', 'Denpasar']
ISPS = ['Indihome', 'FirstMedia', 'Biznet', 'XL', 'Telkomsel']
COUNTRY = 'Indonesia'

CSV_PATH = {
    "users": "./data/data_init_movie_streaming/users.csv",
    "subscriptions": "./data/data_init_movie_streaming/subscriptions.csv",
    "movies": "./data/data_init_movie_streaming/movies.csv",
    "watch_sessions": "./data/data_init_movie_streaming/watch_sessions.csv",
    "payments": "./data/data_init_movie_streaming/payments.csv",
    "ratings": "./data/data_init_movie_streaming/ratings.csv",
    "video_playback_events": "./data/data_init_movie_streaming/video_playback_events.csv"
}

# ---------- GENERATORS ----------

def user_generator(n, mode="initial"):
    last_id = 0 if mode == "initial" else get_last_id("users", "user_id")
    now = datetime.now(ZoneInfo("Asia/Jakarta"))
    start = datetime(2024,1,1) if mode == "initial" else now - timedelta(hours=1)
    data = []
    for i in range(last_id + 1, last_id + n + 1):
        join_date = fake.date_time_between(start, now, tzinfo=ZoneInfo("Asia/Jakarta"))\
            .strftime('%Y-%m-%d %H:%M:%S')
        data.append({
            "user_id": i,
            "name": fake.name(),
            "email": fake.unique.email(),
            "country": COUNTRY,
            "city": random.choice(CITIES),
            "join_date": join_date,
            "subscription_status": random.choice(SUB_STATUS_USER),
            "created_at": join_date
        })
    return pd.DataFrame(data)

def subscriptions_generator(users_df, mode="initial"):
    last_id = 0 if mode == "initial" else get_last_id("subscriptions", "subscription_id")
    data = [] 
    for i, user in enumerate(users_df.itertuples(), start=1):
        plan_type, price = random.choice(PLANS)
        start_date = datetime.strptime(user.join_date, '%Y-%m-%d %H:%M:%S')
        end_date = start_date + timedelta(days=random.randint(30, 365))
        data.append({
            "subscription_id": last_id + i,
            "user_id": user.user_id,
            "plan_type": plan_type,
            "start_date": start_date.strftime('%Y-%m-%d %H:%M:%S'),
            "end_date": end_date.strftime('%Y-%m-%d %H:%M:%S'),
            "price": price,
            "status": random.choice(SUB_STATUS),
            "created_at": start_date.strftime('%Y-%m-%d %H:%M:%S')
        })
    return pd.DataFrame(data)

def movie_generator(n, mode="initial"):
    last_id = 0 if mode == "initial" else get_last_id("movies", "movie_id")
    now = datetime.now(ZoneInfo("Asia/Jakarta"))
    start = datetime(2024,1,1) if mode == "initial" else now - timedelta(hours=1)
    data = []
    for i in range(last_id + 1, last_id + n + 1):
        data.append({
            "movie_id": i,
            "title": fake.sentence(nb_words=3).replace('.', ''),
            "genre": random.choice(GENRES),
            "release_year": random.randint(1990, now.year),
            "duration_min": random.randint(60, 180),
            "language": random.choice(LANGUAGES),
            "created_at": fake.date_time_between(start, now, tzinfo=ZoneInfo("Asia/Jakarta"))\
                .strftime('%Y-%m-%d %H:%M:%S')
        })
    return pd.DataFrame(data)

def watch_sessions_generator(users_df, movies_df, n, mode="initial"):
    last_id = 0 if mode == "initial" else get_last_id("watch_sessions", "session_id")
    now = datetime.now(ZoneInfo("Asia/Jakarta"))
    start = datetime(2024,1,1) if mode == "initial" else now - timedelta(hours=1)
    data = []
    for i in range(last_id + 1, last_id + n + 1):
        user = users_df.sample(1).iloc[0]
        movie = movies_df.sample(1).iloc[0]
        start_time = fake.date_time_between(start, now, tzinfo=ZoneInfo("Asia/Jakarta"))
        duration = timedelta(minutes=random.randint(10, int(movie['duration_min'])))
        end_time = start_time + duration
        completion = round(min(100, (duration.total_seconds() / (movie['duration_min'] * 60)) * 100), 2)
        data.append({
            "session_id": i,
            "user_id": user['user_id'],
            "movie_id": movie['movie_id'],
            "device_type": random.choice(DEVICE_TYPES),
            "start_time": start_time.strftime('%Y-%m-%d %H:%M:%S'),
            "end_time": end_time.strftime('%Y-%m-%d %H:%M:%S'),
            "completion_rate": completion,
            "created_at": start_time.strftime('%Y-%m-%d %H:%M:%S')
        })
    return pd.DataFrame(data)

def payments_generator(subs_df, n, mode="initial"):
    last_id = 0 if mode == "initial" else get_last_id("payments", "payment_id")
    now = datetime.now(ZoneInfo("Asia/Jakarta"))
    start = datetime(2024,1,1) if mode == "initial" else now - timedelta(hours=1)
    data = []
    for i in range(last_id + 1, last_id + n + 1):
        sub = subs_df.sample(1).iloc[0]
        payment_date = fake.date_time_between(start, now, tzinfo=ZoneInfo("Asia/Jakarta"))\
            .strftime('%Y-%m-%d %H:%M:%S')
        data.append({
            "payment_id": i,
            "user_id": sub['user_id'],
            "subscription_id": sub['subscription_id'],
            "amount": sub['price'],
            "payment_date": payment_date,
            "method": random.choice(PAYMENT_METHODS),
            "status": random.choice(PAYMENT_STATUS),
            "created_at": payment_date
        })
    return pd.DataFrame(data)

def ratings_generator(users_df, movies_df, n,  mode="initial"):
    last_id = 0 if mode == "initial" else get_last_id("ratings", "rating_id")
    now = datetime.now(ZoneInfo("Asia/Jakarta"))
    start = datetime(2024,1,1) if mode == "initial" else now - timedelta(hours=1)
    data = []
    for i in range(last_id + 1, last_id + n + 1):
        user = users_df.sample(1).iloc[0]
        movie = movies_df.sample(1).iloc[0]
        rating_date = fake.date_time_between(start, now, tzinfo=ZoneInfo("Asia/Jakarta"))\
            .strftime('%Y-%m-%d %H:%M:%S')
        data.append({
            "rating_id": i,
            "user_id": user['user_id'],
            "movie_id": movie['movie_id'],
            "rating_score": random.randint(1, 5),
            "rating_date": rating_date,
            "review_text": fake.sentence(nb_words=10),
            "created_at": rating_date
        })
    return pd.DataFrame(data)

def save_to_csv(df, filename):
    df.to_csv(filename, index=False)
    print(f"Saved: {filename}")

# ---------- MAIN ----------
def generate_movie_streaming_data(mode="hourly"):
    if mode == "hourly":
        users = user_generator(random.randint(2, 10), mode)
        movies = movie_generator(random.randint(1, 5), mode)
        subs = subscriptions_generator(users, mode)
        watches = watch_sessions_generator(users, movies, random.randint(10, 100), mode)
        payments = payments_generator(subs, random.randint(5, 30), mode)
        ratings = ratings_generator(users, movies, random.randint(2, 20), mode)
        return {
            "users": users,
            "subscriptions": subs,
            "movies": movies,
            "watch_sessions": watches,
            "payments": payments,
            "ratings": ratings
        }
    else:
        users = user_generator(1000, mode)
        movies = movie_generator(600, mode)
        subs = subscriptions_generator(users)
        watches = watch_sessions_generator(users, movies, 10000)
        payments = payments_generator(subs, 3000)
        ratings = ratings_generator(users, movies, 2000)
        save_to_csv(users, CSV_PATH["users"])
        save_to_csv(subs, CSV_PATH["subscriptions"])
        save_to_csv(movies, CSV_PATH["movies"])
        save_to_csv(watches, CSV_PATH["watch_sessions"])
        save_to_csv(payments, CSV_PATH["payments"])
        save_to_csv(ratings, CSV_PATH["ratings"])
