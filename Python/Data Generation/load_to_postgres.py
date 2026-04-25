"""
load_to_postgres.py
NestKraft Export Intelligence - PostgreSQL Data Loader
"""

import pandas as pd
from sqlalchemy import create_engine, text
import os, sys, time
import warnings
warnings.filterwarnings('ignore')

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import CONNECTION_STRING

BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
RAW_PATH  = os.path.join(BASE_DIR, "..", "..", "Data", "Raw")
engine    = create_engine(CONNECTION_STRING)

TABLES = [
    # (csv_filename,                  table_name,               chunk_size)
    ("dim_products.csv",              "dim_products",            None),
    ("dim_markets.csv",               "dim_markets",             None),
    ("dim_customers.csv",             "dim_customers",           None),
    ("dim_forex_rates.csv",           "dim_forex_rates",         None),
    ("fact_export_orders.csv",        "fact_export_orders",      20_000),
    ("fact_shipment_details.csv",     "fact_shipment_details",   20_000),
    ("fact_competitor_pricing.csv",   "fact_competitor_pricing", 20_000),
    ("fact_market_demand.csv",        "fact_market_demand",      20_000),
    ("fact_returns.csv",              "fact_returns",            20_000),
]

DATE_COLS = {
    "dim_products":           ["product_launch_date"],
    "dim_customers":          ["registration_date"],
    "dim_forex_rates":        ["date"],
    "fact_export_orders":     ["order_date","payment_due_date","payment_received_date"],
    "fact_shipment_details":  ["shipment_date","estimated_arrival_date","actual_arrival_date"],
    "fact_competitor_pricing":["date"],
    "fact_market_demand":     ["date"],
    "fact_returns":           ["return_date"],
}

print("\n" + "="*65)
print("    NESTKRAFT - PostgreSQL Data Loader")
print("="*65 + "\n")

total_rows = 0
t_start    = time.time()

for csv_file, table_name, chunk_size in TABLES:
    csv_path    = os.path.join(RAW_PATH, csv_file)
    parse_dates = DATE_COLS.get(table_name, [])
    t0          = time.time()

    try:
        if chunk_size is None:
            df = pd.read_csv(csv_path, parse_dates=parse_dates)
            df.to_sql(table_name, engine, if_exists="append",
                      index=False, method="multi")
            elapsed = round(time.time()-t0, 1)
            print(f"  ✅  {table_name:<38}  {len(df):>8,} rows  |  {elapsed:.1f}s")
            total_rows += len(df)
        else:
            rows_loaded = 0
            reader = pd.read_csv(csv_path, parse_dates=parse_dates,
                                 chunksize=chunk_size)
            print(f"  ⏳  {table_name:<38}  loading...", end="\r")
            for chunk in reader:
                chunk.to_sql(table_name, engine, if_exists="append",
                             index=False, method="multi")
                rows_loaded += len(chunk)
                print(f"  ⏳  {table_name:<38}  {rows_loaded:>8,} rows...", end="\r")
            elapsed = round(time.time()-t0, 1)
            print(f"  ✅  {table_name:<38}  {rows_loaded:>8,} rows  |  {elapsed:.1f}s")
            total_rows += rows_loaded

    except Exception as e:
        print(f"\n  ❌  {table_name} - ERROR: {e}\n")

# ── Verify row counts ─────────────────────────────────────────
print("\n" + "─"*65)
print("  📊  Verifying row counts...\n")
with engine.connect() as conn:
    for _, table_name, _ in TABLES:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
        count  = result.fetchone()[0]
        print(f"       {table_name:<40}  {count:>8,} rows")

total_time = round(time.time()-t_start, 1)
print(f"\n  ✅  Total rows loaded : {total_rows:,}")
print(f"  ⏱  Total time        : {total_time}s")
print("\n" + "="*65 + "\n")