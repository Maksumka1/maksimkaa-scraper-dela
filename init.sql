CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Основна таблиця вантажів
CREATE TABLE IF NOT EXISTS cargo_history (
    request_id VARCHAR(512) PRIMARY KEY,
    route_from VARCHAR(100) NOT NULL,
    route_to VARCHAR(100) NOT NULL,
    route_from_full VARCHAR(250),
    route_to_full VARCHAR(250),
    route_from_region VARCHAR(100),
    route_to_region VARCHAR(100),
    cargo_type VARCHAR(150),
    tags TEXT[] DEFAULT '{}',
    transport_types TEXT[] DEFAULT '{}',
    distance_km INT,
    weight_t NUMERIC(6, 2),
    volume_m3 NUMERIC(6, 2),
    price_uah NUMERIC(10, 2),
    price_per_km_uah NUMERIC(8, 2),
    published_relative VARCHAR(64),
    order_url TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Довідник населених пунктів та областей
CREATE TABLE IF NOT EXISTS geo_locations (
    id SERIAL PRIMARY KEY,
    city_name VARCHAR(100) NOT NULL,
    district_name VARCHAR(100),
    region_name VARCHAR(100) NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_city_region UNIQUE (city_name, region_name)
);

-- Індекси для ILIKE пошуку (GIN Trigram)
CREATE INDEX IF NOT EXISTS idx_cargo_route_from_trgm ON cargo_history USING gin (route_from gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_cargo_route_to_trgm ON cargo_history USING gin (route_to gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_cargo_from_reg_trgm ON cargo_history USING gin (route_from_region gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_cargo_to_reg_trgm ON cargo_history USING gin (route_to_region gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_cargo_transport_types ON cargo_history USING gin (transport_types);
CREATE INDEX IF NOT EXISTS idx_cargo_callback_token
    ON cargo_history ((substr(encode(digest(request_id, 'sha256'), 'hex'), 1, 32)));

-- Індекси для сортування за часом та вибірок
CREATE INDEX IF NOT EXISTS idx_cargo_created_at ON cargo_history (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_cargo_roundtrip ON cargo_history (route_from_region, route_to_region, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_geo_city ON geo_locations (city_name);
CREATE INDEX IF NOT EXISTS idx_geo_region ON geo_locations (region_name);
-- Історія надсилання комплектів «туди + назад»
CREATE TABLE IF NOT EXISTS round_trip_pairs (
    chat_id BIGINT NOT NULL,
    forward_request_id VARCHAR(512) NOT NULL,
    return_request_id VARCHAR(512) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (chat_id, forward_request_id, return_request_id)
);

CREATE INDEX IF NOT EXISTS idx_round_trip_pairs_created_at ON round_trip_pairs (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_round_trip_pairs_forward ON round_trip_pairs (forward_request_id, created_at DESC);

