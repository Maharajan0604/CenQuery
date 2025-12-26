import os
import sys
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# 1. Load Environment Variables
load_dotenv()

# --- CONFIG: Dual Output (Console + File) ---
class DualLogger:
    """Writes output to both the console and a file simultaneously."""
    def __init__(self, filename):
        self.terminal = sys.stdout
        self.log = open(filename, "w", encoding="utf-8")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        # Needed for python 3 compatibility
        self.terminal.flush()
        self.log.flush()

# Force UTF-8 on the terminal first (for emojis)
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

# Redirect stdout and stderr to our DualLogger
# This captures all print() statements and errors
OUTPUT_FILE = "output.txt"
sys.stdout = DualLogger(OUTPUT_FILE)
sys.stderr = sys.stdout 
# --------------------------------------------

# Get the single connection string from .env
DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")

# Check if it exists
if not DB_CONNECTION_STRING:
    print("❌ Error: DB_CONNECTION_STRING not found in .env file.")
    sys.exit(1)

SQL_FILE = "maharajan_queries.sql"

def load_queries(filepath):
    """Reads queries from the file (one query per line)."""
    if not os.path.exists(filepath):
        print(f"❌ Error: {filepath} not found.")
        return []
    
    with open(filepath, 'r', encoding='utf-8') as f:
        # distinct lines, skipping empty ones
        return [line.strip() for line in f if line.strip()]

def verify_queries():
    print(f"📄 Output is being saved to: {os.path.abspath(OUTPUT_FILE)}")
    print("🚀 Connecting to Database...")
    
    try:
        engine = create_engine(DB_CONNECTION_STRING)
        queries = load_queries(SQL_FILE)

        if not queries:
            print("⚠️  No queries found in queries.sql to run.")
            return

        print(f"📋 Found {len(queries)} queries. Starting verification...\n")

        with engine.connect() as conn:
            for index, sql_query in enumerate(queries, 1):
                print(f"🔹 Running Query #{index}...")
                
                try:
                    # Execute query
                    result = conn.execute(text(sql_query))
                    
                    # Check for data
                    if result.returns_rows:
                        rows = result.fetchall()
                        df = pd.DataFrame(rows, columns=list(result.keys()))
                        
                        if not df.empty:
                            print(f"✅ Success! Returned {len(df)} rows.")
                            # to_string handles formatting nicely for text files
                            print(df.head().to_string(index=False)) 
                        else:
                            print("⚠️  Query ran successfully but returned NO data.")
                            print(f"Query: {sql_query}")
                    else:
                        print("✅ Query executed (No rows returned).")

                except Exception as q_err:
                    print(f"❌ Query #{index} Failed: {q_err}")
                    conn.rollback()
                
                print("-" * 50)

    except Exception as e:
        print(f"\n❌ Database Connection Error: {e}")

if __name__ == "__main__":
    verify_queries()