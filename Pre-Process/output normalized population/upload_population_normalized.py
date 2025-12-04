import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

load_dotenv()

# ==========================================
# 🔧 CONFIGURATION
# ==========================================
REGIONS_FILE = "output_normalized/regions.csv"
STATS_FILE = "output_normalized/population_stats.csv"

# Fetch variables
USER = os.getenv("user")
PASSWORD = os.getenv("password")
HOST = os.getenv("host")
PORT = os.getenv("port")
DBNAME = os.getenv("dbname")

# Construct Connection String
DB_CONNECTION_STRING = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}?sslmode=require"

def enable_rls(table_name, engine):
    """Secures the table with RLS."""
    print(f"  🔒 Securing table '{table_name}'...")
    try:
        with engine.begin() as conn:
            conn.execute(text(f'ALTER TABLE "{table_name}" ENABLE ROW LEVEL SECURITY;'))
            conn.execute(text(f'DROP POLICY IF EXISTS "Public Read Access" ON "{table_name}";'))
            conn.execute(text(f'CREATE POLICY "Public Read Access" ON "{table_name}" FOR SELECT USING (true);'))
        print(f"     ✅ Security applied.")
    except Exception as e:
        print(f"     ⚠️ Error applying security: {e}")

def upload_regions(engine):
    """Uploads regions and sets the Primary Key."""
    if not os.path.exists(REGIONS_FILE):
        print(f"❌ Error: {REGIONS_FILE} not found.")
        return False

    print(f"\n🚀 Processing PARENT table: 'regions'...")
    df = pd.read_csv(REGIONS_FILE)
    
    try:
        # 1. Upload Data
        df.to_sql('regions', engine, if_exists='replace', index=False, chunksize=1000)
        print(f"     ✅ Data uploaded ({len(df)} rows).")
        
        # 2. Add Primary Key Constraint (Crucial for Foreign Keys to work)
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE regions ADD PRIMARY KEY (state);"))
        print(f"     ✅ Primary Key (state) enforced.")
        
        enable_rls('regions', engine)
        return True
        
    except Exception as e:
        print(f"     ❌ Error uploading regions: {e}")
        return False

def upload_stats(engine):
    """Uploads stats and links them to regions via Foreign Key."""
    if not os.path.exists(STATS_FILE):
        print(f"❌ Error: {STATS_FILE} not found.")
        return False

    print(f"\n🚀 Processing CHILD table: 'population_stats'...")
    df = pd.read_csv(STATS_FILE)
    
    try:
        # 1. Upload Data
        df.to_sql('population_stats', engine, if_exists='replace', index=False, chunksize=1000)
        print(f"     ✅ Data uploaded ({len(df)} rows).")
        
        # 2. Add Foreign Key Constraint
        # This ensures 'state' in this table actually exists in the 'regions' table
        with engine.begin() as conn:
            conn.execute(text("""
                ALTER TABLE population_stats 
                ADD CONSTRAINT fk_regions 
                FOREIGN KEY (state) REFERENCES regions(state);
            """))
        print(f"     ✅ Foreign Key linked to 'regions' table.")
        
        enable_rls('population_stats', engine)
        return True
        
    except Exception as e:
        print(f"     ❌ Error uploading stats: {e}")
        return False

# ==========================================
# 🏁 MAIN EXECUTION
# ==========================================
if __name__ == "__main__":
    
    # Setup DB Engine
    try:
        engine = create_engine(DB_CONNECTION_STRING, poolclass=NullPool)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ Connected to Supabase successfully.")
    except Exception as e:
        print(f"❌ FATAL: Connection failed. {e}")
        exit()

    # 1. Upload Parent Table FIRST
    success = upload_regions(engine)
    
    # 2. Upload Child Table SECOND (Only if parent succeeded)
    if success:
        upload_stats(engine)
    else:
        print("\n⚠️ Skipping 'population_stats' because 'regions' failed.")