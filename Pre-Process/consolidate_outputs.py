import os
import shutil
import pandas as pd

# ==========================================
# 🔧 CONFIGURATION
# ==========================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "unified_outputs")

# Mapping of source folders to the files we want to grab
SOURCES = {
    "output_normalized_healthcare": [
        "regions.csv",          # MASTER COPY: Contains 39 states
        "healthcare_stats.csv",
        "tru.csv"               # MASTER COPY
    ],
    "output_normalized_population": [
        "population_stats.csv",
    ],
    "output_normalized_education": [
        "pca_stats.csv"
    ],
    "output_normalized_religion": [
        "religion_stats.csv",
        "religions.csv"
    ],
    "output_normalized_occupation": [
        "occupation_stats.csv",
        "age_groups.csv"
    ],
    "output_normalized_language": [
        "language_stats.csv",
        "languages.csv"
    ],
    "output_normalized_crops": [  # FIX: Updated to use underscore name
        "crops.csv"
    ]
}

def consolidate():
    # 1. Create the unified folder
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"📁 Created directory: {OUTPUT_DIR}")
    else:
        print(f"📁 Directory exists: {OUTPUT_DIR}")

    print("-" * 40)

    # 2. Iterate and Copy
    copied_count = 0
    for folder, files in SOURCES.items():
        src_folder_path = os.path.join(BASE_DIR, folder)
        
        if not os.path.exists(src_folder_path):
            print(f"⚠️  Warning: Source folder not found: {folder}")
            continue

        for filename in files:
            src_file = os.path.join(src_folder_path, filename)
            dst_file = os.path.join(OUTPUT_DIR, filename)

            if os.path.exists(src_file):
                shutil.copy2(src_file, dst_file)
                print(f"✅ Copied: {filename:<25} (from {folder})")
                copied_count += 1
            else:
                print(f"❌ Missing: {filename:<25} (in {folder})")

    # 3. Create a README to document the schema
    readme_path = os.path.join(OUTPUT_DIR, "README.txt")
    with open(readme_path, "w") as f:
        f.write("UNIFIED CENSUS DATA STAGING AREA\n")
        f.write("================================\n\n")
        f.write("Standardized Keys:\n")
        f.write("- State ID: From regions.csv (0-38)\n")
        f.write("- TRU ID: 1=Total, 2=Rural, 3=Urban\n")
    
    print("-" * 40)
    print(f"🎉 Success! {copied_count} files consolidated into 'unified_outputs/'.")

if __name__ == "__main__":
    consolidate()