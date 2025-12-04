import pandas as pd
import re
import os

# ==========================================
# 🔧 CONFIGURATION
# ==========================================
INPUT_FILE = "input/population.xls" 
OUTPUT_DIR = "output_population"
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "population.csv")

os.makedirs(OUTPUT_DIR, exist_ok=True)

def clean_column_name(name):
    """Standardizes column names: lowercase, underscores, no special chars."""
    if not name: return "col"
    s = str(name).lower()
    s = s.strip()
    s = re.sub(r'\s+', '_', s)            # Replace spaces with underscore
    s = re.sub(r'[^a-z0-9_]', '', s)      # Remove special chars (like in 'Distt.')
    return s[:60]

def process_population_data(input_path):
    print(f"📖 Reading: {input_path}")
    
    df = None
    try:
        # Try reading as Excel first (since extension is .xls)
        df = pd.read_excel(input_path)
        print("   ✅ Detected Excel format.")
    except Exception:
        try:
            # Fallback to CSV if Excel fails
            df = pd.read_csv(input_path)
            print("   ✅ Detected CSV format.")
        except FileNotFoundError:
            print(f"❌ Error: File not found at {input_path}")
            return None
        except Exception as e:
            print(f"❌ Error reading file: {e}")
            return None

    # 1. Clean Column Names
    # e.g., "Total Persons" -> "total_persons"
    df.columns = [clean_column_name(c) for c in df.columns]
    
    # 2. DROP THE 'TABLE' COLUMN (Redundant)
    if 'table' in df.columns:
        print("   ✂️ Dropping 'table' column...")
        df.drop(columns=['table'], inplace=True)
    
    # 3. Convert Float Population Columns to Integers
    # We identify population columns by keywords
    pop_cols = [c for c in df.columns if 'persons' in c or 'males' in c or 'females' in c]
    
    print(f"   Converting {len(pop_cols)} columns to Integers...")
    for col in pop_cols:
        # Fill NaNs with 0 before converting, then cast to integer
        df[col] = df[col].fillna(0).astype(int)

    # 4. Clean Age Column
    # The raw file has '0.0', '1.0' mixed with 'All ages'. 
    if 'age' in df.columns:
        df['age'] = df['age'].astype(str).str.replace('.0', '', regex=False)

    return df

# ==========================================
# 🏁 MAIN
# ==========================================
if __name__ == "__main__":
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Input file not found: {INPUT_FILE}")
        print("   Please check the 'input' folder.")
        exit()

    df_clean = process_population_data(INPUT_FILE)
    
    if df_clean is not None:
        # Save CSV
        df_clean.to_csv(OUTPUT_CSV, index=False)
        print(f"💾 Saved Clean CSV to: {OUTPUT_CSV}")
        
        print("\n--- First 5 Rows ---")
        print(df_clean.head())