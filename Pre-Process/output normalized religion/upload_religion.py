import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

load_dotenv()

# ==========================================
# 🔧 CONFIGURATION
# ==========================================
RELIGIONS_FILE = "output_normalized/religions.csv"
TRU_FILE = "output_normalized/tru.csv"
STATS_FILE = "output_normalized/religion_stats.csv"
TABLE_NAME = "religion_stats"

# Fetch variables
USER = os.getenv("user")
PASSWORD = os.getenv("password")
HOST = os.getenv("host")
PORT = os.getenv("port")
DBNAME = os.getenv("dbname")

DB_CONNECTION_STRING = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}?sslmode=require"

def enable_rls(table_name, engine):
    print(f"  🔒 Securing '{table_name}'...")
    try:
        with engine.begin() as conn:
            conn.execute(text(f'ALTER TABLE "{table_name}" ENABLE ROW LEVEL SECURITY;'))
            conn.execute(text(f'DROP POLICY IF EXISTS "Public Read Access" ON "{table_name}";'))
            conn.execute(text(f'CREATE POLICY "Public Read Access" ON "{table_name}" FOR SELECT USING (true);'))
        print(f"     ✅ Security applied.")
    except Exception as e:
        print(f"     ⚠️ Error: {e}")

def upload_lookups(engine):
    # 1. Upload Religions Lookup
    if os.path.exists(RELIGIONS_FILE):
        print(f"\n🚀 Uploading 'religions' lookup...")
        df_rel = pd.read_csv(RELIGIONS_FILE)
        try:
            df_rel.to_sql('religions', engine, if_exists='replace', index=False)
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE religions ADD PRIMARY KEY (id);"))
            enable_rls('religions', engine)
            print("     ✅ Uploaded 'religions'")
        except Exception as e:
            print(f"     ❌ Error uploading religions: {e}")

    # 2. Upload TRU Lookup (Skip if it exists to avoid breaking PCA FKs)
    # We try to create it only if it doesn't exist, or we can use 'if_exists="append"' if we are sure IDs match.
    # Safe bet: If you ran PCA upload, 'tru' exists. If not, this will create it.
    if os.path.exists(TRU_FILE):
        print(f"\n🚀 Checking 'tru' lookup...")
        df_tru = pd.read_csv(TRU_FILE)
        try:
            # We use 'replace' ONLY if we are sure no other table uses it yet. 
            # Since PCA might use it, this part can be tricky. 
            # Best practice: Try to upload, handle error if table exists.
            # For simplicity in this script, we assume we can overwrite or it matches.
            # If PCA is already up, 'replace' will FAIL due to Foreign Key constraint.
            # So we catch that specific error.
            df_tru.to_sql('tru', engine, if_exists='replace', index=False)
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE tru ADD PRIMARY KEY (id);"))
            enable_rls('tru', engine)
            print("     ✅ Uploaded/Updated 'tru'")
        except Exception as e:
            print(f"     ℹ️  'tru' table likely exists (linked to PCA). Skipping overwrite.")

def upload_stats(engine):
    if not os.path.exists(STATS_FILE):
        print(f"❌ Error: {STATS_FILE} not found.")
        return

    print(f"\n🚀 Uploading DATA table: '{TABLE_NAME}'...")
    df_iter = pd.read_csv(STATS_FILE, chunksize=5000)
    
    try:
        first_chunk = True
        total_rows = 0
        for chunk in df_iter:
            mode = 'replace' if first_chunk else 'append'
            chunk.to_sql(TABLE_NAME, engine, if_exists=mode, index=False)
            first_chunk = False
            total_rows += len(chunk)
            print(f"     ... uploaded {total_rows} rows")
            
        # Add ALL Foreign Keys
        print("🔗 Linking Foreign Keys...")
        with engine.begin() as conn:
            # 1. Link to Regions
            conn.execute(text(f"""
                ALTER TABLE {TABLE_NAME} 
                ADD CONSTRAINT fk_regions_rel 
                FOREIGN KEY (state) REFERENCES regions(state);
            """))
            print("     ✅ Linked to 'regions'")

            # 2. Link to Religions
            conn.execute(text(f"""
                ALTER TABLE {TABLE_NAME} 
                ADD CONSTRAINT fk_religions_lookup 
                FOREIGN KEY (religion_id) REFERENCES religions(id);
            """))
            print("     ✅ Linked to 'religions'")

            # 3. Link to TRU
            conn.execute(text(f"""
                ALTER TABLE {TABLE_NAME} 
                ADD CONSTRAINT fk_tru_rel 
                FOREIGN KEY (tru_id) REFERENCES tru(id);
            """))
            print("     ✅ Linked to 'tru'")
        
        enable_rls(TABLE_NAME, engine)
        
    except Exception as e:
        print(f"     ❌ Error uploading stats: {e}")

if __name__ == "__main__":
    try:
        engine = create_engine(DB_CONNECTION_STRING, poolclass=NullPool)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ Connected to Supabase.")
        
        upload_lookups(engine)
        upload_stats(engine)
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")