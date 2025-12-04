import pandas as pd
import re
import os

# ==========================================
# 🔧 CONFIGURATION
# ==========================================
INPUT_FILE = "input/Occupation.xls"
OUTPUT_DIR = "output_normalized_occupation"

# Dimension Files (Lookups)
TRU_FILE = os.path.join(OUTPUT_DIR, "tru.csv")
REGIONS_FILE = os.path.join(OUTPUT_DIR, "regions.csv")
AGE_GROUPS_FILE = os.path.join(OUTPUT_DIR, "age_groups.csv")

# Fact File (Data)
OCCUPATION_STATS_FILE = os.path.join(OUTPUT_DIR, "occupation_stats.csv")

os.makedirs(OUTPUT_DIR, exist_ok=True)

def clean_text(text):
    """Removes noise like '(01)' or 'State - ' from names."""
    if not isinstance(text, str): return text
    text = str(text).replace('State - ', '')
    text = re.sub(r'\s*\(\d+\)', '', text)
    return text.strip()

def process_occupation_data():
    print(f"📖 Reading: {INPUT_FILE}")
    
    # 1. Load Data with Manual Header Mapping
    column_names = [
        "table_code", "state_code", "district_code", "area_name", "tru", "age_group",
        "population_total", "population_male", "population_female",
        "main_workers_total", "main_workers_male", "main_workers_female",
        "marginal_workers_total", "marginal_workers_male", "marginal_workers_female",
        "marg_3_6mo_total", "marg_3_6mo_male", "marg_3_6mo_female",
        "marg_less_3mo_total", "marg_less_3mo_male", "marg_less_3mo_female",
        "non_workers_total", "non_workers_male", "non_workers_female",
        "seeking_work_total", "seeking_work_male", "seeking_work_female"
    ]

    try:
        # Using read_excel for .xls files
        df = pd.read_excel(
            INPUT_FILE, 
            skiprows=9, 
            header=None, 
            names=column_names,
            dtype={'state_code': str, 'district_code': str} # Keep codes as strings
        )
    except Exception as e:
        print(f"❌ Error reading Excel file: {e}")
        return

    # 2. Basic Cleaning
    df = df.dropna(subset=['state_code'])
    df['area_name'] = df['area_name'].apply(clean_text)
    df['age_group'] = df['age_group'].replace('Total', 'All Ages')
    
    # ==========================================
    # 🌟 STEP 1: EXTRACT DIMENSIONS (Lookups)
    # ==========================================
    
    # --- A. TRU Lookup (Total/Rural/Urban) ---
    print("✂️  Extracting TRU Lookup...")
    unique_tru = df['tru'].unique()
    tru_df = pd.DataFrame({
        'id': range(1, len(unique_tru) + 1),
        'name': unique_tru
    })
    tru_df.to_csv(TRU_FILE, index=False)
    print(f"   ✅ Created '{TRU_FILE}'")

    # Map TRU to ID in main dataframe
    tru_map = dict(zip(tru_df['name'], tru_df['id']))
    df['tru_id'] = df['tru'].map(tru_map)

    # --- B. Regions Lookup (State/District) ---
    print("✂️  Extracting Regions Lookup...")
    # We save this BEFORE dropping columns so the master regions table is complete
    regions_df = df[['state_code', 'district_code', 'area_name']].drop_duplicates()
    regions_df.to_csv(REGIONS_FILE, index=False)
    print(f"   ✅ Created '{REGIONS_FILE}'")

    # --- C. Age Group Lookup ---
    print("✂️  Extracting Age Group Lookup...")
    unique_ages = df['age_group'].unique()
    age_df = pd.DataFrame({
        'id': range(1, len(unique_ages) + 1),
        'name': unique_ages
    })
    age_df.to_csv(AGE_GROUPS_FILE, index=False)
    print(f"   ✅ Created '{AGE_GROUPS_FILE}'")

    # Map Age Group to ID in main dataframe
    age_map = dict(zip(age_df['name'], age_df['id']))
    df['age_group_id'] = df['age_group'].map(age_map)

    # ==========================================
    # 🌟 STEP 2: NORMALIZE & REORDER FACT TABLE
    # ==========================================
    
    # 1. Drop redundant columns
    # We remove district_code (as requested), area_name, tru, age_group, and table_code
    cols_to_drop = ['area_name', 'tru', 'age_group', 'district_code', 'table_code']
    df.drop(columns=[c for c in cols_to_drop if c in df.columns], inplace=True)

    # 2. Ensure numeric columns are integers
    numeric_cols = [c for c in df.columns if 'total' in c or 'male' in c or 'female' in c]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    # 3. Reorder Columns: State -> TRU -> Age -> Data
    # Identify metric columns (everything that is not a key)
    keys = ['state_code', 'tru_id', 'age_group_id']
    metrics = [c for c in df.columns if c not in keys]
    
    # Apply new column order
    df = df[keys + metrics]

    # Save the cleaned satellite table
    df.to_csv(OCCUPATION_STATS_FILE, index=False)
    print(f"   ✅ Created '{OCCUPATION_STATS_FILE}' (Fact Table)")
    print(f"   Columns: {list(df.columns)}")
    print(f"📊 Processed {len(df)} rows.")

if __name__ == "__main__":
    if os.path.exists(INPUT_FILE):
        process_occupation_data()
    else:
        print(f"❌ File not found: {INPUT_FILE}")