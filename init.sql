CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS cargo_history (
    request_id VARCHAR(128) PRIMARY KEY,
    route_from VARCHAR(100) NOT NULL,
    route_to VARCHAR(100) NOT NULL,
    cargo_type VARCHAR(150),
    distance_km INT,
    weight_t NUMERIC(5, 2),
    volume_m3 NUMERIC(6, 2),
    price_uah NUMERIC(10, 2),
    price_per_km_uah NUMERIC(8, 2),
    published_relative VARCHAR(64),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- GIN Trigram-індекси для прискорення ILIKE пошуку
CREATE INDEX IF NOT EXISTS idx_cargo_route_from_trgm ON cargo_history USING gin (route_from gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_cargo_route_to_trgm ON cargo_history USING gin (route_to gin_trgm_ops);

-- B-Tree індекс для відсікання діапазону 48 годин та сортування
CREATE INDEX IF NOT EXISTS idx_cargo_created_at ON cargo_history (created_at DESC);