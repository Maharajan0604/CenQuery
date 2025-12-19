import json
import os
import sys
import re

# ==========================================
# 🔧 CONFIGURATION
# ==========================================
SCHEMA_FILE = "database_schema.json"  # Your schema file
QUESTIONS_FILE = "questions.txt"      # File containing list of questions
SQL_FILE = "queries.sql"              # File containing list of SQL queries
OUTPUT_DIR = "training_data"

def load_schema_string(schema_path):
    """Reads the JSON schema and converts it into a compact SQL DDL string."""
    if not os.path.exists(schema_path):
        print(f"❌ Error: Schema file not found at {schema_path}")
        sys.exit(1)

    with open(schema_path, 'r') as f:
        schema_json = json.load(f)

    ddl_statements = []
    for table_name, details in schema_json.items():
        columns = []
        for col in details.get('columns', []):
            col_def = f"{col['name']} {col['type']}"
            if "PK" in col.get('constraints', []):
                col_def += " PRIMARY KEY"
            columns.append(col_def)
        table_def = f"CREATE TABLE {table_name} ({', '.join(columns)});"
        ddl_statements.append(table_def)

    return "\n".join(ddl_statements)

def format_training_entry(question, sql, schema_string):
    """Formats the entry into the exact prompt structure required."""
    prompt = f"""### Task
Generate a SQL query to answer the following question:
`{question}`

### Database Schema
This query will run on a database whose schema is represented in this string:
{schema_string}

### SQL
{sql}"""
    return {"text": prompt}

def load_questions(filepath):
    """Reads questions line by line."""
    if not os.path.exists(filepath):
        print(f"❌ Error: Questions file not found at {filepath}")
        return []
    with open(filepath, 'r', encoding='utf-8') as f:
        return [line.strip() for line in f if line.strip()]

def load_sql_queries(filepath):
    """
    Reads SQL file, splits by semicolon to handle multi-line queries,
    and flattens them into single lines.
    """
    if not os.path.exists(filepath):
        print(f"❌ Error: SQL file not found at {filepath}")
        return []
    
    with open(filepath, 'r', encoding='utf-8') as f:
        raw_content = f.read()

    # Split by semicolon (assuming each query ends with ;)
    # This handles the multi-line issue automatically
    raw_queries = raw_content.split(';')
    
    cleaned_queries = []
    for q in raw_queries:
        # Replace newlines with spaces and strip whitespace
        flattened = q.replace('\n', ' ').strip()
        # Remove multiple spaces
        flattened = re.sub(' +', ' ', flattened)
        
        if flattened:
            cleaned_queries.append(flattened + ';') # Add semicolon back
            
    return cleaned_queries

def get_unique_filename(directory, filename):
    """Adds (1), (2) etc. if file exists."""
    base, ext = os.path.splitext(filename)
    counter = 1
    new_filename = filename
    while os.path.exists(os.path.join(directory, new_filename)):
        new_filename = f"{base}({counter}){ext}"
        counter += 1
    return os.path.join(directory, new_filename)

def main():
    print("==================================================")
    print("🤖 CENQUERY ROBUST GENERATOR")
    print("==================================================")

    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
    
    member_name = input("Enter your name (e.g., Member3): ").strip().replace(" ", "_")
    if not member_name: member_name = "Member_Unknown"
    
    base_filename = f"train_{member_name}_final.jsonl"

    # Load Data
    schema_string = load_schema_string(SCHEMA_FILE)
    print(f"✅ Schema loaded.")

    questions = load_questions(QUESTIONS_FILE)
    queries = load_sql_queries(SQL_FILE)

    print(f"📊 Loaded {len(questions)} questions")
    print(f"📊 Loaded {len(queries)} valid SQL queries (Multi-line fixed)")

    # Validation
    if len(questions) != len(queries):
        print(f"⚠️  MISMATCH: {len(questions)} questions vs {len(queries)} queries.")
        print("❌ Please check your files. The script tried to auto-fix newlines, but counts still don't match.")
        # Optional: Print mismatch details if needed
        sys.exit(1)

    if len(questions) == 0:
        print("❌ Files are empty.")
        sys.exit(1)

    # Save
    output_path = get_unique_filename(OUTPUT_DIR, base_filename)
    
    with open(output_path, 'w', encoding='utf-8') as f_out:
        for q, s in zip(questions, queries):
            entry = format_training_entry(q, s, schema_string)
            f_out.write(json.dumps(entry) + "\n")

    print("--------------------------------------------------")
    print(f"✅ Success! Generated {len(questions)} training examples.")
    print(f"📂 Output saved to: {output_path}")
    print("==================================================")

if __name__ == "__main__":
    main()