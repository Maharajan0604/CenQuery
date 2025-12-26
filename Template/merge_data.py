import os
import glob

# ==========================================
# 🔧 CONFIGURATION
# ==========================================
INPUT_DIR = "training_data_final"
OUTPUT_FILE = "consolidated_train.jsonl"

def consolidate_jsonl():
    print(f"🚀 Consolidating JSONL files from {INPUT_DIR}...")
    
    if not os.path.exists(INPUT_DIR):
        print(f"❌ Error: Directory '{INPUT_DIR}' not found.")
        return

    files = glob.glob(os.path.join(INPUT_DIR, "train_*.jsonl"))
    
    if not files:
        print("⚠️  No JSONL files found. Have your team run the generator script first?")
        return

    total_lines = 0
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as outfile:
        for file_path in files:
            print(f"   📄 Processing {os.path.basename(file_path)}...")
            try:
                with open(file_path, 'r', encoding='utf-8') as infile:
                    for line in infile:
                        if line.strip(): 
                            outfile.write(line)
                            total_lines += 1
            except Exception as e:
                print(f"   ❌ Error reading {file_path}: {e}")

    print("--------------------------------------------------")
    print(f"✅ Success! Merged {len(files)} files into '{OUTPUT_FILE}'.")
    print(f"📊 Total Training Examples: {total_lines}")
    print("--------------------------------------------------")

if __name__ == "__main__":
    consolidate_jsonl()