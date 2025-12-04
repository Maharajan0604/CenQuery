
-- 1. Regions Lookup (Parent)
CREATE TABLE regions (
    state BIGINT PRIMARY KEY,
    area_name TEXT
);

-- 2. Population Data (Child)
CREATE TABLE population_stats (
    state BIGINT,
    age TEXT,
    total_persons BIGINT,
    total_males BIGINT,
    total_females BIGINT,
    rural_persons BIGINT,
    rural_males BIGINT,
    rural_females BIGINT,
    urban_persons BIGINT,
    urban_males BIGINT,
    urban_females BIGINT,
    FOREIGN KEY (state) REFERENCES regions (state)
);
