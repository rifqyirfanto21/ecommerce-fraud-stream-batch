CREATE TABLE IF NOT EXISTS raw_users (
    user_id        BIGSERIAL PRIMARY KEY,
    name           VARCHAR(200) NOT NULL,
    email          VARCHAR(200) NOT NULL UNIQUE,
    phone_number   VARCHAR(50),
    created_date   TIMESTAMPTZ NOT NULL,
    ingestion_ts   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source         VARCHAR(50) NOT NULL DEFAULT 'batch'
);

CREATE TABLE IF NOT EXISTS raw_products (
    product_id     BIGSERIAL PRIMARY KEY,
    product_name   VARCHAR(255) NOT NULL UNIQUE,
    brand          VARCHAR(100),
    category       VARCHAR(100),
    sub_category   VARCHAR(100),
    currency       VARCHAR(10),
    price          BIGINT NOT NULL,
    cost           BIGINT NOT NULL,
    created_date   TIMESTAMPTZ NOT NULL,
    ingestion_ts   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source         VARCHAR(50) NOT NULL DEFAULT 'batch'
);

CREATE TABLE IF NOT EXISTS raw_orders (
    order_id        VARCHAR(50) PRIMARY KEY,
    user_id         VARCHAR(50) NOT NULL,
    product_id      VARCHAR(50) NOT NULL,
    quantity        INT NOT NULL,
    amount          VARCHAR(50) NOT NULL,
    amount_numeric  BIGINT NOT NULL,
    country         VARCHAR(10) NOT NULL,
    status          VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_date    TIMESTAMPTZ NOT NULL,
    event_ts        TIMESTAMPTZ NOT NULL,
    source          VARCHAR(50) NOT NULL,
    ingestion_ts    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);