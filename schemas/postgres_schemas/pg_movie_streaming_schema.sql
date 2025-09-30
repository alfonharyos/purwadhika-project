-- ==========================
-- SCHEMA: MOVIE STREAMING
-- ==========================

-- 1. USERS
CREATE TABLE IF NOT EXISTS users (
    user_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(150) UNIQUE NOT NULL,
    country VARCHAR(100) NOT NULL,
    city VARCHAR(100),
    join_date TIMESTAMP NOT NULL DEFAULT NOW(),
    subscription_status VARCHAR(20) NOT NULL CHECK (subscription_status IN ('active','inactive')),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    run_date_pg DATE NOT NULL DEFAULT CURRENT_DATE
);

-- 2. SUBSCRIPTIONS
CREATE TABLE IF NOT EXISTS subscriptions (
    subscription_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    plan_type VARCHAR(50) NOT NULL CHECK (plan_type IN ('Basic','Standard','Premium')),
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP NOT NULL,
    price NUMERIC(10,2) NOT NULL CHECK (price > 0),
    status VARCHAR(20) NOT NULL CHECK (status IN ('active','expired')),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    run_date_pg DATE NOT NULL DEFAULT CURRENT_DATE
);

-- 3. MOVIES
CREATE TABLE IF NOT EXISTS movies (
    movie_id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    genre VARCHAR(50) NOT NULL,
    release_year INT NOT NULL CHECK (release_year BETWEEN 1900 AND EXTRACT(YEAR FROM NOW())),
    duration_min INT NOT NULL CHECK (duration_min > 0),
    language VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    run_date_pg DATE NOT NULL DEFAULT CURRENT_DATE
);

-- 4. WATCH_SESSIONS (batch)
CREATE TABLE IF NOT EXISTS watch_sessions (
    session_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    movie_id INT NOT NULL REFERENCES movies(movie_id) ON DELETE CASCADE,
    device_type VARCHAR(50) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    completion_rate NUMERIC(5,2) CHECK (completion_rate >= 0 AND completion_rate <= 100),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    run_date_pg DATE NOT NULL DEFAULT CURRENT_DATE
);

-- 5. PAYMENTS
CREATE TABLE IF NOT EXISTS payments (
    payment_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    subscription_id INT NOT NULL REFERENCES subscriptions(subscription_id) ON DELETE CASCADE,
    amount NUMERIC(10,2) NOT NULL CHECK (amount > 0),
    payment_date TIMESTAMP NOT NULL,
    method VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('pending','completed','failed')),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    run_date_pg DATE NOT NULL DEFAULT CURRENT_DATE
);

-- 6. RATINGS
CREATE TABLE IF NOT EXISTS ratings (
    rating_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    movie_id INT NOT NULL REFERENCES movies(movie_id) ON DELETE CASCADE,
    rating_score INT NOT NULL CHECK (rating_score BETWEEN 1 AND 5),
    rating_date TIMESTAMP NOT NULL,
    review_text TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    run_date_pg DATE NOT NULL DEFAULT CURRENT_DATE
);