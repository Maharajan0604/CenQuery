import pandas as pd
import re
import os

# ==========================================
# 🔧 CONFIGURATION
# ==========================================
INPUT_FILE = "input/education.xls"
OUTPUT_DIR = "output_normalized_education"
TRU_FILE = os.path.join(OUTPUT_DIR, "tru.csv")
PCA_STATS_FILE = os.path.join(OUTPUT_DIR, "pca_stats.csv")

os.makedirs(OUTPUT_DIR, exist_ok=True)

def clean_column_name(name):
    if not name: return "col"
    s = str(name).lower().strip()
    s = re.sub(r'\s+', '_', s)
    s = re.sub(r'[^a-z0-9_]', '', s)
    s = s.replace('population_person', 'person')
    s = s.replace('population_male', 'male')
    s = s.replace('population_female', 'female')
    return s[:60]

def process_pca_data():
    print(f"📖 Reading: {INPUT_FILE}")
    try:
        df = pd.read_csv(INPUT_FILE)
    except:
        try:
            df = pd.read_excel(INPUT_FILE)
        except Exception as e:
            print(f"❌ Error: {e}")
            return

    # 1. Clean Column Names
    df.columns = [clean_column_name(c) for c in df.columns]
    if 'state_code' in df.columns:
        df.rename(columns={'state_code': 'state'}, inplace=True)
    
    # 2. Extract TRU Lookup
    print("✂️  Extracting TRU Lookup...")
    unique_tru = df['tru'].unique()
    tru_df = pd.DataFrame({
        'id': range(1, len(unique_tru) + 1),
        'name': unique_tru
    })
    tru_df.to_csv(TRU_FILE, index=False)
    print(f"   ✅ Created '{TRU_FILE}'")

    # 3. Map TRU to ID
    tru_map = dict(zip(tru_df['name'], tru_df['id']))
    df['tru_id'] = df['tru'].map(tru_map)

    # 4. Drop Redundant Columns
    cols_to_drop = [
        'district_code', 'subdistt_code', 'townvillage_code',
        'state_code1', 'district_code1', 'subdistt_code1', 'townvillage_code1',
        'ward_code', 'eb_code', 'level', 'name', 'tru' # Drop original text TRU
    ]
    df.drop(columns=[c for c in cols_to_drop if c in df.columns], inplace=True)

    # 5. Clean State Code
    if 'state' in df.columns:
        df['state'] = df['state'].fillna(0).astype(int)

    # Save Stats
    df.to_csv(PCA_STATS_FILE, index=False)
    print(f"   ✅ Created '{PCA_STATS_FILE}'")

if __name__ == "__main__":
    if os.path.exists(INPUT_FILE):
        process_pca_data()
    else:
        print(f"❌ File not found: {INPUT_FILE}")