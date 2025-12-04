import pdfplumber
import pandas as pd
import os

# ==========================================
# 🔧 CONFIGURATION
# ==========================================
INPUT_PDF = "input/Crops.pdf"
OUTPUT_DIR = "output_3"
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "crops_2.csv")

os.makedirs(OUTPUT_DIR, exist_ok=True)

def extract_crops_data(pdf_path):
    print(f"📖 Opening PDF: {pdf_path}")
    
    with pdfplumber.open(pdf_path) as pdf:
        page = pdf.pages[0]
        table = page.extract_table()
        
    if not table:
        print("❌ No table found.")
        return None

    df = pd.DataFrame(table)

    # =======================================================
    # 🧠 STRATEGY: Explicit Column Selection
    # Based on your logs, the data is in these exact indices:
    # 0: Crop Name ("Rice")
    # 2: Normal Area ("403.09")
    # 5: Area Sown 2025-26 ("438.51")
    # 8: Area Sown 2024-25 ("430.06")
    # 11: Difference ("8.45")
    # 14: % Change ("1.97")
    # =======================================================
    
    # 1. Select only the columns that actually have the data
    target_indices = [0, 2, 5, 8, 11, 14]
    
    # Verify we have enough columns before selecting
    if df.shape[1] <= max(target_indices):
        print(f"⚠️ Table width ({df.shape[1]}) is smaller than expected.")
        return None

    df_clean = df.iloc[:, target_indices].copy()

    # 2. Hardcode the Correct Headers
    clean_headers = [
        "crop",
        "normal_area_dafw",
        "area_sown_2025_26",
        "area_sown_2024_25",
        "difference_area",
        "pct_increase_decrease"
    ]
    df_clean.columns = clean_headers

    # 3. Clean Data Rows
    # Discard the metadata rows (0-8)
    # Row 9 is where "Rice" starts
    df_clean = df_clean.iloc[9:].reset_index(drop=True)
    
    # 4. Clean Cell Values
    # Remove newlines and trim whitespace
    df_clean = df_clean.replace(r'\n', ' ', regex=True)
    
    # Remove rows where 'crop' is empty or None
    df_clean = df_clean[df_clean['crop'].notna() & (df_clean['crop'] != "")]
    
    # Remove the "Grand Total" or "Total" rows if you want pure data
    # (Optional: comment this out if you want to keep totals)
    # df_clean = df_clean[~df_clean['crop'].str.contains("Total", case=False, na=False)]

    return df_clean

# ==========================================
# 🏁 MAIN
# ==========================================
if __name__ == "__main__":
    df_result = extract_crops_data(INPUT_PDF)
    
    if df_result is not None:
        df_result.to_csv(OUTPUT_CSV, index=False)
        print(f"\n💾 Saved Clean CSV to: {OUTPUT_CSV}")
        print("\n--- Preview ---")
        print(df_result.head())