import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

load_dotenv()

FILE_NAME = "output_crop/crops.csv"
TABLE_NAME = "crops"

# Fetch variables
USER = os.getenv("user")
PASSWORD = os.getenv("password")
HOST = os.getenv("host")
PORT = os.getenv("port")
DBNAME = os.getenv("dbname")
# Construct the SQLAlchemy connection string
DB_CONNECTION_STRING = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}?sslmode=require"

def enable_rls(table_name, engine):
    """
    Enables Row Level Security (RLS) on the table and creates a 
    policy that allows public read access (SELECT) for everyone.
    """
    print(f"  🔒 Securing table '{table_name}'...")
    try:
        # Use a transaction to ensure all settings apply together
        with engine.begin() as conn:
            # 1. Enable RLS (Removes "Unrestricted" warning in Supabase)
            conn.execute(text(f'ALTER TABLE "{table_name}" ENABLE ROW LEVEL SECURITY;'))
            
            # 2. Clean up old policies to prevent errors on re-runs
            conn.execute(text(f'DROP POLICY IF EXISTS "Public Read Access" ON "{table_name}";'))
            
            # 3. Create a policy: Allow "anon" (public) to SELECT data
            conn.execute(text(f'CREATE POLICY "Public Read Access" ON "{table_name}" FOR SELECT USING (true);'))
            
        print(f"     ✅ Security applied: Public Read / Admin Write.")
    except Exception as e:
        print(f"     ⚠️ Error applying security: {e}")

def upload_to_db(df, table_name, engine):
    """
    Uploads a DataFrame to PostgreSQL and immediately secures it.
    """
    print(f"  🚀 Uploading {len(df)} rows to table '{table_name}'...")
    try:
        # if_exists='replace': Drops the table if it exists and creates a new one
        # chunksize=1000: Prevents timeouts with large files
        df.to_sql(table_name, engine, if_exists='replace', index=False, chunksize=1000)
        
        print(f"     ✅ Success! Table '{table_name}' created/replaced.")
        
        # Immediately apply security settings after creation
        enable_rls(table_name, engine)
        
    except Exception as e:
        print(f"     ❌ Database Error during upload: {e}")


if __name__ == "__main__":

    # 1. Check if file exists
    if not os.path.exists(FILE_NAME):
        print(f"❌ Error: File not found at '{FILE_NAME}'")
        print("   Please check the folder name (e.g., is it 'output_3' or 'output_crop'?)")
        exit()

    # 2. Setup DB Engine
    try:
        engine = create_engine(DB_CONNECTION_STRING, poolclass=NullPool)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ Connected to Supabase successfully.\n")
    except Exception as e:
        print(f"❌ FATAL: Could not connect to Supabase. Check connection string.\nError: {e}")
        exit()

    # 3. Read CSV and Upload
    try:
        print(f"📖 Reading file: {FILE_NAME}")
        df = pd.read_csv(FILE_NAME) # <--- Reading the data here 
        
        if not df.empty:
            upload_to_db(df, TABLE_NAME, engine)
        else:
            print("⚠️ The CSV file is empty.")
            
    except Exception as e:
        print(f"❌ Error reading or uploading file: {e}")