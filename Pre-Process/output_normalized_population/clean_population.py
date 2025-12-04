import pandas as pd
import re
import os

# ==========================================
# 🔧 CONFIGURATION
# ==========================================
INPUT_FILE = "input/population.xls"

# Intermediate Output (Cleaned Data)
# INTERMEDIATE_DIR = "output_population"
# INTERMEDIATE_CSV = os.path.join(INTERMEDIATE_DIR, "population.csv")

# Final Output (Normalized Data)
FINAL_OUTPUT_DIR = "output_normalized_population"

# Create directories
# os.makedirs(INTERMEDIATE_DIR, exist_ok=True)
os.makedirs(FINAL_OUTPUT_DIR, exist_ok=True)


# ==========================================
# 🛠️ HELPER FUNCTIONS
# ==========================================
def clean_column_name(name):
    """Standardizes column names: lowercase, underscores, no special chars."""
    if not name: return "col"
    s = str(name).lower()
    s = s.strip()
    s = re.sub(r'\s+', '_', s)            # Replace spaces with underscore
    s = re.sub(r'[^a-z0-9_]', '', s)      # Remove special chars (like in 'Distt.')
    return s[:60]


# ==========================================
# 🚀 STEP 1: CLEANING & PROCESSING
# ==========================================
def process_population_data(input_path):
    print(f"📖 [Step 1] Reading: {input_path}")
    
    df = None
    try:
        # Try reading as Excel first
        df = pd.read_excel(input_path)
        print("   ✅ Detected Excel format.")
    except Exception:
        try:
            # Fallback to CSV
            df = pd.read_csv(input_path)
            print("   ✅ Detected CSV format.")
        except FileNotFoundError:
            print(f"   ❌ Error: File not found at {input_path}")
            return None
        except Exception as e:
            print(f"   ❌ Error reading file: {e}")
            return None

    # 1. Clean Column Names
    df.columns = [clean_column_name(c) for c in df.columns]
    
    # 2. DROP THE 'TABLE' COLUMN (Redundant)
    if 'table' in df.columns:
        print("   ✂️ Dropping 'table' column...")
        df.drop(columns=['table'], inplace=True)
    
    # 3. Convert Float Population Columns to Integers
    pop_cols = [c for c in df.columns if 'persons' in c or 'males' in c or 'females' in c]
    
    print(f"   Converting {len(pop_cols)} columns to Integers...")
    for col in pop_cols:
        df[col] = df[col].fillna(0).astype(int)

    # 4. Clean Age Column
    if 'age' in df.columns:
        df['age'] = df['age'].astype(str).str.replace('.0', '', regex=False)

    return df


# ==========================================
# 🚀 STEP 2: NORMALIZATION
# ==========================================
def normalize_data(df):
    print(f"\n📖 [Step 2] Normalizing Data...")

    # 1. Drop 'distt' if it exists
    if 'distt' in df.columns:
        print("   ✂️ Dropping redundant 'distt' column...")
        df.drop(columns=['distt'], inplace=True)

    # 2. Create REGIONS Table (State Code -> Area Name)
    # Filter for unique state/area pairs
    regions = df[['state', 'area_name']].drop_duplicates().sort_values(by=['state'])
    
    regions_path = os.path.join(FINAL_OUTPUT_DIR, "regions.csv")
    regions.to_csv(regions_path, index=False)
    print(f"   ✅ Created 'regions.csv' ({len(regions)} rows)")

    # 3. Create STATS Table (Data)
    # Drop 'area_name' because it's now in the regions table
    stats = df.drop(columns=['area_name'])
    
    stats_path = os.path.join(FINAL_OUTPUT_DIR, "population_stats.csv")
    stats.to_csv(stats_path, index=False)
    print(f"   ✅ Created 'population_stats.csv' ({len(stats)} rows)")

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
    sql_path = os.path.join(FINAL_OUTPUT_DIR, "normalized_schema.sql")
    with open(sql_path, "w") as f:
        f.write(sql_schema)
    print(f"   📜 Saved Optimized SQL Schema to '{sql_path}'")


# ==========================================
# 🏁 MAIN EXECUTION
# ==========================================
if __name__ == "__main__":
    # --- Check Input ---
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Input file not found: {INPUT_FILE}")
        print("   Please check the 'input' folder.")
        exit()

    # --- Run Step 1: Process ---
    df_clean = process_population_data(INPUT_FILE)
    
    if df_clean is not None:
        # Save the intermediate clean CSV
        # df_clean.to_csv(INTERMEDIATE_CSV, index=False)
        # print(f"💾 Saved Clean CSV to: {INTERMEDIATE_CSV}")
        
        # --- Run Step 2: Normalize ---
        # Pass the dataframe directly to the next step
        normalize_data(df_clean)
        
        print("\n🎉 All processing complete.")