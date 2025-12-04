import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

load_dotenv()

# ==========================================
# 🔧 CONFIGURATION
# ==========================================
# These match the output of your cleaning script
REGIONS_FILE = "output_normalized/regions.csv"
TRU_FILE = "output_normalized/tru.csv"
STATS_FILE = "output_normalized/healthcare_stats.csv"
TABLE_NAME = "healthcare_stats"

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

def update_regions(engine):
    """
    Reads local regions.csv and appends ONLY new IDs to the database.
    This handles cases where new states (e.g. Telangana) were added during cleaning.
    """
    print(f"\n🚀 Checking 'regions' table for updates...")
    if not os.path.exists(REGIONS_FILE):
        print(f"❌ Error: {REGIONS_FILE} not found.")
        return

    df = pd.read_csv(REGIONS_FILE)
    
    try:
        # Get existing IDs from DB
        with engine.connect() as conn:
            # Check if table exists first
            result = conn.execute(text("SELECT to_regclass('public.regions')"))
            if result.scalar() is None:
                 print("     ℹ️  Regions table missing. Creating fresh...")
                 df.to_sql('regions', engine, if_exists='replace', index=False)
                 with engine.begin() as conn:
                    conn.execute(text("ALTER TABLE regions ADD PRIMARY KEY (state);"))
                 enable_rls('regions', engine)
                 return

            existing_ids = pd.read_sql("SELECT state FROM regions", conn)['state'].tolist()
        
        # Filter for new rows
        new_rows = df[~df['state'].isin(existing_ids)]
        
        if not new_rows.empty:
            print(f"     ➕ Found {len(new_rows)} new regions to add.")
            new_rows.to_sql('regions', engine, if_exists='append', index=False)
            print("     ✅ Regions updated.")
        else:
            print("     ✅ Regions table is already up to date.")
            
    except Exception as e:
        print(f"     ❌ Error updating regions: {e}")

def upload_tru(engine):
    print(f"\n🚀 Checking 'tru' lookup table...")
    if not os.path.exists(TRU_FILE):
        print(f"❌ Error: {TRU_FILE} not found.")
        return False
        
    df = pd.read_csv(TRU_FILE)
    try:
        with engine.connect() as conn:
            # Check if table exists
            result = conn.execute(text("SELECT to_regclass('public.tru')"))
            if result.scalar() is None:
                 df.to_sql('tru', engine, if_exists='replace', index=False)
                 with engine.begin() as conn:
                    conn.execute(text("ALTER TABLE tru ADD PRIMARY KEY (id);"))
                 enable_rls('tru', engine)
                 print("     ✅ Created 'tru' table.")
                 return True
            
            existing_ids = pd.read_sql("SELECT id FROM tru", conn)['id'].tolist()
            
        new_rows = df[~df['id'].isin(existing_ids)]
        if not new_rows.empty:
             new_rows.to_sql('tru', engine, if_exists='append', index=False)
             print(f"     ✅ Added {len(new_rows)} new TRU types.")
        else:
             print("     ✅ TRU table is up to date.")
        return True
    except Exception as e:
        print(f"     ❌ Error with TRU table: {e}")
        return False

def upload_stats(engine):
    print(f"\n🚀 Uploading DATA table: '{TABLE_NAME}'...")
    if not os.path.exists(STATS_FILE):
        print(f"❌ Error: {STATS_FILE} not found.")
        return

    # Healthcare files are wide, so we chunk them
    df_iter = pd.read_csv(STATS_FILE, chunksize=2000)
    
    try:
        first_chunk = True
        total_rows = 0
        for chunk in df_iter:
            mode = 'replace' if first_chunk else 'append'
            chunk.to_sql(TABLE_NAME, engine, if_exists=mode, index=False)
            first_chunk = False
            total_rows += len(chunk)
            print(f"     ... uploaded {total_rows} rows")
            
        # Add Foreign Keys
        print("🔗 Linking Foreign Keys...")
        with engine.begin() as conn:
            # Link to Regions
            conn.execute(text(f"""
                ALTER TABLE {TABLE_NAME} 
                ADD CONSTRAINT fk_regions_health 
                FOREIGN KEY (state) REFERENCES regions(state);
            """))
            print("     ✅ Linked to 'regions'")

            # Link to TRU
            conn.execute(text(f"""
                ALTER TABLE {TABLE_NAME} 
                ADD CONSTRAINT fk_tru_health 
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
        
        # 1. Update Lookups First
        update_regions(engine)
        if upload_tru(engine):
            # 2. Upload Data
            upload_stats(engine)
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")