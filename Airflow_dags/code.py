import hashlib, json, os, subprocess, sys
from datetime import datetime
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator

# --- CONFIGURATION (DYNAMIC ROOT WORKSPACE DISCOVERY) ---
DAG_DIR     = Path(os.path.abspath(os.path.dirname(__file__)))
ROOT_DIR    = DAG_DIR.parent.resolve()

INPUT_FILE  = ROOT_DIR / "DATA" / "2018.csv" 
JSON_FILE   = ROOT_DIR / "DATA" / "2018_supplement.json"
PARQUET     = ROOT_DIR / "DATA" / "flights_ingested.parquet"
DB_PATH     = ROOT_DIR / "DATA" / "flights_star_schema.db"
DB_URL      = f"sqlite:///{DB_PATH}"

def install_dependencies():
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--user", "openpyxl", "pyarrow", "fastparquet", "sqlalchemy"])

def apply_schema():
    from sqlalchemy import create_engine, text
    engine = create_engine(DB_URL)
    tables = [
        "CREATE TABLE IF NOT EXISTS airlines (carrier_code TEXT PRIMARY KEY, carrier_name TEXT);",
        "CREATE TABLE IF NOT EXISTS airports (airport_code TEXT PRIMARY KEY, airport_name TEXT, city TEXT, state TEXT);",
        "CREATE TABLE IF NOT EXISTS flights (flight_id TEXT PRIMARY KEY, fl_date TIMESTAMP, carrier_code TEXT, origin TEXT, destination TEXT, cancelled INTEGER);",
        "CREATE TABLE IF NOT EXISTS flight_delays (flight_id TEXT, weather_delay FLOAT, late_aircraft_delay FLOAT);"
    ]
    with engine.begin() as conn:
        for sql in tables:
            conn.execute(text(sql))

def ingest_and_validate():
    import pandas as pd
    
    try:
        df = pd.read_excel(INPUT_FILE, engine="openpyxl")
    except:
        df = pd.read_csv(INPUT_FILE, encoding='latin1', on_bad_lines='skip')

    df.columns = df.columns.str.upper().str.strip()

    
    # CRITERION 5: INITIAL BEFORE STATISTICS
    print("=== STAGE 4 PREPROCESSING: INITIAL STATE ===")
    print(f"Original Dataframe Shape: {df.shape}")
    print(f"Initial Missing Values Per Column:\n{df.isnull().sum()}\n")


    
    # 1. Date Check & Row Removal Strategy
    before_date_count = df.shape[0]
    df["FL_DATE"] = pd.to_datetime(df["FL_DATE"], errors="coerce")
    df = df.dropna(subset=["FL_DATE"])
    print(f"[Strategy - Removal]: Dropped {before_date_count - df.shape[0]} rows due to missing/corrupt FL_DATE values.")

    # 2. Delay Imputation Strategy (LATE_AIRCRAFT_DELAY)
    if 'LATE_AIRCRAFT_DELAY' in df.columns:
        null_delays = df['LATE_AIRCRAFT_DELAY'].isnull().sum()
        df['LATE_AIRCRAFT_DELAY'] = df['LATE_AIRCRAFT_DELAY'].fillna(0.0)
        print(f"[Strategy - Imputation]: Imputed {null_delays} blank delay fields with 0.0 (No delay occurred).")


    for col in ["OP_CARRIER", "ORIGIN", "DEST"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.upper().str.strip()
    print("[Scaling & Normalization]: Cleaned whitespaces and normalized codes to uppercase for categorical alignment.")


    df["flight_id"] = (df["FL_DATE"].astype(str) + 
                      df.get("OP_CARRIER", "").astype(str) + 
                      df.get("OP_CARRIER_FL_NUM", "").astype(str) + 
                      df.get("ORIGIN", "").astype(str)).apply(lambda x: hashlib.md5(x.encode()).hexdigest())
    
    shape_before_dedup = df.shape[0]
    df = df.drop_duplicates(subset="flight_id")
    print(f"[Strategy - Deduplication]: Dropped {shape_before_dedup - df.shape[0]} duplicate rows based on unique flight_id.")
    
    # CRITERION 5: FINAL AFTER-STATISTICS
    print("\n=== STAGE 4 PREPROCESSING: FINAL STATE ===")
    print(f"Cleaned Dataframe Shape: {df.shape}")
    print(f"Final Missing Values Per Column:\n{df.isnull().sum()}")
    print("============================================")
    
    df.to_parquet(PARQUET, index=False)

def load_to_database():
    import pandas as pd
    from sqlalchemy import create_engine
    engine = create_engine(DB_URL)

    # 1. Load Dimensions
    if JSON_FILE.exists():
        with open(JSON_FILE) as f:
            sup = json.load(f)
        pd.DataFrame(sup["airlines"]).to_sql("airlines", engine, if_exists="replace", index=False)
        pd.DataFrame(sup["airports"]).to_sql("airports", engine, if_exists="replace", index=False)

    # 2. Load Facts
    df = pd.read_parquet(PARQUET)
    
    flights_cols = {"FL_DATE": "fl_date", "OP_CARRIER": "carrier_code", "DEST": "destination", "CANCELLED": "cancelled", "ORIGIN": "origin"}
    existing_cols = [c for c in flights_cols.keys() if c in df.columns]
    
    flights_df = df[["flight_id"] + existing_cols].rename(columns=flights_cols)
    flights_df.to_sql("flights", engine, if_exists="replace", index=False)

    if 'LATE_AIRCRAFT_DELAY' in df.columns:
        delays = df[["flight_id", "LATE_AIRCRAFT_DELAY"]].rename(columns={"LATE_AIRCRAFT_DELAY": "late_aircraft_delay"})
        delays["weather_delay"] = 0.0
        delays.to_sql("flight_delays", engine, if_exists="replace", index=False)

# CRITERION: Run on a daily schedule at 2:00 AM (0 2 * * *) [cite: 28, 33]
with DAG(
    "flight_ingestion", 
    start_date=datetime(2026,1,1), 
    schedule_interval="0 2 * * *", 
    catchup=False
) as dag:
    t0 = PythonOperator(task_id="install_deps", python_callable=install_dependencies)
    t1 = PythonOperator(task_id="apply_schema", python_callable=apply_schema)
    t2 = PythonOperator(task_id="ingest_and_validate", python_callable=ingest_and_validate)
    t3 = PythonOperator(task_id="load_to_database", python_callable=load_to_database)
    
    t0 >> t1 >> t2 >> t3