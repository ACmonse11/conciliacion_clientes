import pandas as pd
import numpy as np

def pick_column(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

def to_money(series):
    s = series.astype(str)
    s = s.str.replace(",", "", regex=False)
    s = s.str.replace("$", "", regex=False)
    s = s.str.strip()
    return pd.to_numeric(s, errors="coerce")

def to_date(series):
    # Primero intentar ISO / normal
    fechas = pd.to_datetime(series, errors="coerce")

    mask = fechas.isna()
    if mask.any():
        numeric = pd.to_numeric(series, errors="coerce")
        fechas.loc[mask & numeric.notna()] = pd.to_datetime(
            numeric[mask & numeric.notna()],
            unit="D",
            origin="1899-12-30",
            errors="coerce"
        )

    return fechas
