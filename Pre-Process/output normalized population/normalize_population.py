import pandas as pd
import os

# ==========================================
# 🔧 CONFIGURATION
# ==========================================
INPUT_CLEAN_FILE = "output_population/population.csv"
OUTPUT_DIR = "output_normalized"

os.makedirs(OUTPUT_DIR, exist_ok=True)

def normalize_no_distt():
    print(f"📖 Reading Clean Data: {INPUT_CLEAN_FILE}")
    if not os.path.exists(INPUT_CLEAN_FILE):
        print("❌ Clean file not found. Run clean_population.py first!")
        return

    df = pd.read_csv(INPUT_CLEAN_FILE)

    # 1. Drop 'distt' if it exists (User confirmed it is always 0)
    if 'distt' in df.columns:
        print("✂️  Dropping redundant 'distt' column...")
        df.drop(columns=['distt'], inplace=True)

    # 2. Create REGIONS Table (State Code -> Area Name)
    # Now we only need State and Area Name
    regions = df[['state', 'area_name']].drop_duplicates().sort_values(by=['state'])
    
    # Save Regions
    regions_path = os.path.join(OUTPUT_DIR, "regions.csv")
    regions.to_csv(regions_path, index=False)
    print(f"✅ Created 'regions.csv' ({len(regions)} rows)")

    # 3. Create STATS Table (Data)
    # We drop 'area_name' because it serves as the text label for the state ID
    stats = df.drop(columns=['area_name'])
    
    # Save Stats
    stats_path = os.path.join(OUTPUT_DIR, "population_stats.csv")
    stats.to_csv(stats_path, index=False)
    print(f"✅ Created 'population_stats.csv' ({len(stats)} rows)")

    # 4. Generate Optimized SQL Schema
    sql_schema = """
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
"""
    sql_path = os.path.join(OUTPUT_DIR, "normalized_schema.sql")
    with open(sql_path, "w") as f:
        f.write(sql_schema)
    print(f"📜 Saved Optimized SQL Schema to '{sql_path}'")

if __name__ == "__main__":
    normalize_no_distt()