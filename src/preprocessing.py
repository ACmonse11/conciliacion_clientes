import pandas as pd
import numpy as np

def pick_column(df, candidates):
    """
    Devuelve la primera columna que coincida con la lista de candidatos
    """
    cols = list(df.columns)
    for c in candidates:
        if c in cols:
            return c
    return None

def to_money(series):
    """
    Convierte montos con $, comas y texto a número
    """
    s = series.astype(str)
    s = s.str.replace(",", "", regex=False)
    s = s.str.replace("$", "", regex=False)
    s = s.str.strip()
    return pd.to_numeric(s, errors="coerce")

def to_date(series):
    """
    Conversión robusta de fechas bancarias:
    - DD/MM/YYYY
    - DDMMYYYY (01122025)
    - fechas con hora
    - seriales Excel
    - ignora texto basura (OPERACIÓN, SALDO)
    """

    s = series.copy()
    s_str = s.astype(str).str.strip()

    # limpiar textos basura
    s_str = s_str.replace({
        "OPERACIÓN": np.nan,
        "OPERACION": np.nan,
        "SALDO": np.nan,
        "": np.nan,
        "NAN": np.nan
    })

    # formato DDMMYYYY
    mask_ddmmyyyy = s_str.str.fullmatch(r"\d{8}", na=False)

    fechas_ddmmyyyy = pd.to_datetime(
        s_str[mask_ddmmyyyy],
        format="%d%m%Y",
        errors="coerce"
    )

    fechas_general = pd.to_datetime(
        s_str[~mask_ddmmyyyy],
        errors="coerce",
        dayfirst=True
    )

    fechas = pd.concat([fechas_ddmmyyyy, fechas_general]).sort_index()

    # serial Excel SOLO si es numérico
    mask_na = fechas.isna()
    if mask_na.any():
        numeric = pd.to_numeric(s[mask_na], errors="coerce")
        serial_mask = numeric.notna()

        fechas_serial = pd.to_datetime(
            numeric[serial_mask],
            unit="D",
            origin="1899-12-30",
            errors="coerce"
        )

        fechas.loc[fechas_serial.index] = fechas_serial

    return fechas
