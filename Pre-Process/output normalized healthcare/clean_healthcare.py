import pandas as pd
import re
import os

# ==========================================
# 🔧 CONFIGURATION
# ==========================================
INPUT_FILE = "input/Healthcare.xls"
OUTPUT_DIR = "output_normalized"

# Master Files (Shared across all datasets)
REGIONS_FILE = os.path.join(OUTPUT_DIR, "regions.csv")
TRU_FILE = os.path.join(OUTPUT_DIR, "tru.csv")

# Output for this specific dataset
STATS_FILE = os.path.join(OUTPUT_DIR, "healthcare_stats.csv")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ==========================================
# 🧹 CLEANING FUNCTIONS
# ==========================================
def clean_column_name(name):
    """Shortens verbose healthcare column names."""
    if not name: return "col"
    s = str(name).lower().strip()
    
    # Standard replacements
    replacements = {
        'per 1,000 live births': '',
        'per 100,000 live births': '',
        'percentage': 'pct',
        'population': 'pop',
        'households': 'hh',
        'children under age 5 years': 'child_u5',
        'women age 15-49 years': 'women_15_49',
        'men age 15-54 years': 'men_15_54',
        'literate': 'lit',
        'antenatal': 'anc',
        'postnatal': 'pnc',
        'institutional': 'inst',
        'out-of-pocket': 'oop',
        'expenditure': 'exp'
    }
    
    for old, new in replacements.items():
        s = s.replace(old, new)
    
    s = re.sub(r'\([^)]*\)', '', s)   # Remove (content)
    s = re.sub(r'[^a-z0-9_]', '_', s) # Special chars to underscore
    s = re.sub(r'_+', '_', s).strip('_') # Remove duplicates
    
    return s[:60]

def clean_state_name(name):
    """Standardizes state names to match the Master Regions table."""
    if not isinstance(name, str): return ""
    name = name.strip()
    # Remove prefixes found in other files
    name = re.sub(r'^State - ', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s\(\d+\)$', '', name)
    
    name = name.lower()
    name = name.replace('&', 'and')
    
    # Fix specific typos in Healthcare file
    if 'maharastra' in name: return 'maharashtra'
    # Dadra & Nagar Haveli and Daman & Diu are merged in this file
    if 'dadra' in name and 'daman' in name: return 'dadra and nagar haveli and daman and diu'
    
    return name.title()

# ==========================================
# 🚀 MAIN PROCESS
# ==========================================
def process_data():
    print(f"📖 Reading: {INPUT_FILE}")
    try:
        df = pd.read_excel(INPUT_FILE)
    except:
        print("❌ Error reading file.")
        return

    # 1. Clean Column Names
    df.columns = [clean_column_name(c) for c in df.columns]
    
    # 2. Process Regions (States)
    # ---------------------------
    print("🗺️  Processing Regions...")
    
    # Load existing master regions if available
    if os.path.exists(REGIONS_FILE):
        regions_df = pd.read_csv(REGIONS_FILE)
        next_id = regions_df['state'].max() + 1
    else:
        regions_df = pd.DataFrame(columns=['state', 'area_name'])
        next_id = 1

    # Create a normalized lookup map for existing regions
    # { 'andhra pradesh': 28, ... }
    regions_df['norm_name'] = regions_df['area_name'].apply(lambda x: clean_state_name(str(x)).lower())
    name_to_id = dict(zip(regions_df['norm_name'], regions_df['state']))

    # Identify and Add New Regions from this file
    df['clean_state'] = df['states_uts'].apply(lambda x: clean_state_name(str(x)))
    
    new_regions = []
    for state_name in df['clean_state'].unique():
        norm_name = state_name.lower()
        if norm_name not in name_to_id:
            print(f"   ➕ Adding new region: {state_name}")
            name_to_id[norm_name] = next_id
            new_regions.append({'state': next_id, 'area_name': state_name})
            next_id += 1
            
    # Save updated regions file
    if new_regions:
        new_regions_df = pd.DataFrame(new_regions)
        regions_df = pd.concat([regions_df.drop(columns=['norm_name']), new_regions_df], ignore_index=True)
        regions_df.to_csv(REGIONS_FILE, index=False)
        print(f"   ✅ Updated {REGIONS_FILE}")

    # Map IDs to the dataframe
    df['state'] = df['clean_state'].str.lower().map(name_to_id)

    # 3. Process TRU (Area)
    # ---------------------
    print("🏙️  Processing TRU (Area)...")
    if os.path.exists(TRU_FILE):
        tru_df = pd.read_csv(TRU_FILE)
    else:
        # Create from scratch if missing
        unique_tru = df['area'].unique()
        tru_df = pd.DataFrame({'id': range(1, len(unique_tru) + 1), 'name': unique_tru})
        tru_df.to_csv(TRU_FILE, index=False)
        
    # Map TRU IDs
    tru_map = dict(zip(tru_df['name'], tru_df['id']))
    df['tru_id'] = df['area'].map(tru_map)

    # 4. Final Polish & Save
    # ----------------------
    # Drop the original text columns now that we have IDs
    cols_to_drop = ['states_uts', 'area', 'clean_state']
    df.drop(columns=cols_to_drop, inplace=True, errors='ignore')
    
    # Reorder IDs to front
    cols = list(df.columns)
    if 'state' in cols: cols.insert(0, cols.pop(cols.index('state')))
    if 'tru_id' in cols: cols.insert(1, cols.pop(cols.index('tru_id')))
    df = df[cols]

    df.to_csv(STATS_FILE, index=False)
    print(f"✅ Success! Saved stats to: {STATS_FILE}")
    print(f"   Rows: {len(df)}")
    print(f"   Columns: {len(df.columns)}")

if __name__ == "__main__":
    process_data()