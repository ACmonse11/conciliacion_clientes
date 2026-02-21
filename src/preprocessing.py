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
    s = series.astype(str).str.strip()

    # 1️⃣ Intentar parseo automático
    fechas = pd.to_datetime(s, errors="coerce")

    # 2️⃣ Si falló, intentar dayfirst
    mask = fechas.isna()
    if mask.any():
        fechas_dayfirst = pd.to_datetime(
            s[mask],
            errors="coerce",
            dayfirst=True
        )
        fechas.loc[mask] = fechas_dayfirst

    # 3️⃣ Si sigue fallando, intentar número Excel
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