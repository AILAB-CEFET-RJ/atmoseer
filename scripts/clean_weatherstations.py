"""Clean config/WeatherStations.csv and save cleaned CSV and parquet.

Cleans:
- Removes unnamed index column
- Normalizes decimal commas in VL_ALTITUDE to dot and coerces to float
- Coerces VL_LATITUDE / VL_LONGITUDE to float
- Normalizes DT_INICIO_OPERACAO from dd/mm/yyyy to ISO YYYY-MM-DD (when present)
- Strips whitespace and lowercases STATION_ID
- Saves cleaned CSV and parquet to config/
"""
import pandas as pd
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
INPUT = ROOT / "config" / "WeatherStations.csv"
OUT_CSV = ROOT / "config" / "WeatherStations.cleaned.csv"
OUT_PARQUET = ROOT / "config" / "WeatherStations.cleaned.parquet"


def parse_date(d):
    if pd.isna(d):
        return pd.NaT
    d = str(d).strip()
    if not d:
        return pd.NaT
    # expected dd/mm/yyyy
    try:
        return pd.to_datetime(d, dayfirst=True, errors="coerce").date()
    except Exception:
        return pd.NaT


def clean():
    if not INPUT.exists():
        print(f"Input not found: {INPUT}")
        sys.exit(1)

    df = pd.read_csv(INPUT, dtype=str)

    # Drop unnamed index if present (first empty column)
    if df.columns[0].startswith("Unnamed") or df.columns[0] == "":
        df = df.drop(df.columns[0], axis=1)

    # Standardize column names
    df.columns = [c.strip() for c in df.columns]

    # Clean station id
    if "STATION_ID" in df.columns:
        df["STATION_ID"] = df["STATION_ID"].astype(str).str.strip().str.lower()

    # Clean latitude/longitude
    for col in ["VL_LATITUDE", "VL_LONGITUDE"]:
        if col in df.columns:
            # remove quotes and coerce
            df[col] = df[col].astype(str).str.replace('"', '')
            df[col] = df[col].str.replace(',', '.')
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Clean altitude: replace comma decimal separators, remove quotes, coerce
    if "VL_ALTITUDE" in df.columns:
        df["VL_ALTITUDE"] = df["VL_ALTITUDE"].astype(str).str.replace('"', '')
        df["VL_ALTITUDE"] = df["VL_ALTITUDE"].str.replace(',', '.')
        df["VL_ALTITUDE"] = pd.to_numeric(df["VL_ALTITUDE"], errors="coerce")

    # Clean date
    if "DT_INICIO_OPERACAO" in df.columns:
        df["DT_INICIO_OPERACAO"] = df["DT_INICIO_OPERACAO"].apply(parse_date)

    # Strip station name and status
    for col in ["DC_NOME", "SG_ESTADO", "CD_SITUACAO"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # Reorder columns to a sensible order if exists
    desired = ["STATION_ID", "DC_NOME", "SG_ESTADO", "CD_SITUACAO", "VL_LATITUDE", "VL_LONGITUDE", "VL_ALTITUDE", "DT_INICIO_OPERACAO"]
    cols = [c for c in desired if c in df.columns]
    rest = [c for c in df.columns if c not in cols]
    df = df[cols + rest]

    # Save
    df.to_csv(OUT_CSV, index=False)
    try:
        df.to_parquet(OUT_PARQUET, index=False)
    except Exception as e:
        print("Parquet save failed (pyarrow or fastparquet may be missing):", e)

    # Summary
    print("Saved cleaned CSV to:", OUT_CSV)
    if OUT_PARQUET.exists():
        print("Saved cleaned Parquet to:", OUT_PARQUET)
    print("Rows:", len(df))
    print(df.dtypes)


if __name__ == '__main__':
    clean()
