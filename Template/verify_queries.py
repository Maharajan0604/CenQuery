import os
import sys
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# 1. Load Environment Variables
load_dotenv()

# Get the single connection string from .env
DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")

# Check if it exists
if not DB_CONNECTION_STRING:
    print("❌ Error: DB_CONNECTION_STRING not found in .env file.")
    sys.exit(1)

SQL_FILE = "queries.sql"

def load_queries(filepath):
    """Reads queries from the file (one query per line)."""
    if not os.path.exists(filepath):
        print(f"❌ Error: {filepath} not found.")
        return []
    
    with open(filepath, 'r', encoding='utf-8') as f:
        # distinct lines, skipping empty ones
        return [line.strip() for line in f if line.strip()]

def verify_queries():
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
                        df = pd.DataFrame(result.fetchall(), columns=list(result.keys()))
                        if not df.empty:
                            print(f"✅ Success! Returned {len(df)} rows.")
                            print(df.head().to_string(index=False)) # Show top 5 rows
                        else:
                            print("⚠️  Query ran successfully but returned NO data.")
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