import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

load_dotenv()

TRU_FILE = "output_normalized_education/tru.csv"
PCA_STATS_FILE = "output_normalized_education/pca_stats.csv"

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

def upload_tru(engine):
    print(f"\n🚀 Uploading LOOKUP table: 'tru'...")
    df = pd.read_csv(TRU_FILE)
    try:
        df.to_sql('tru', engine, if_exists='replace', index=False)
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE tru ADD PRIMARY KEY (id);"))
        enable_rls('tru', engine)
        print("     ✅ Uploaded 'tru'")
        return True
    except Exception as e:
        print(f"     ❌ Error: {e}")
        return False

def upload_pca(engine):
    print(f"\n🚀 Uploading DATA table: 'pca_stats'...")
    # Using chunksize for large files
    df_iter = pd.read_csv(PCA_STATS_FILE, chunksize=5000)
    
    try:
        first_chunk = True
        for chunk in df_iter:
            mode = 'replace' if first_chunk else 'append'
            chunk.to_sql('pca_stats', engine, if_exists=mode, index=False)
            first_chunk = False
            print("     ... chunk uploaded")
            
        # Add Foreign Keys
        with engine.begin() as conn:
            # Link to Regions
            conn.execute(text("""
                ALTER TABLE pca_stats 
                ADD CONSTRAINT fk_regions_pca 
                FOREIGN KEY (state) REFERENCES regions(state);
            """))
            print("     ✅ Linked to 'regions'")

            # Link to TRU
            conn.execute(text("""
                ALTER TABLE pca_stats 
                ADD CONSTRAINT fk_tru_pca 
                FOREIGN KEY (tru_id) REFERENCES tru(id);
            """))
            print("     ✅ Linked to 'tru'")
            
        enable_rls('pca_stats', engine)
        
    except Exception as e:
        print(f"     ❌ Error: {e}")

if __name__ == "__main__":
    try:
        engine = create_engine(DB_CONNECTION_STRING, poolclass=NullPool)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        if upload_tru(engine):
            upload_pca(engine)
            
    except Exception as e:
        print(f"❌ Connection failed: {e}")