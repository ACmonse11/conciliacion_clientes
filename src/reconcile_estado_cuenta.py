import re
import pandas as pd

from .preprocessing import pick_column, to_money, to_date
from .config import (
    CARGO_COL_CANDIDATES,
    FECHA_COL_CANDIDATES,
    EGRESO_MONTO_CANDIDATES,
)

RESTRICTED_PUE_FORMA = {
    "EFECTIVO",
    "TARJETA CREDITO",
    "TARJETA CRÉDITO",
    "CONDONACION",
    "CONDONACIÓN",
    "NOVACION",
    "NOVACIÓN",
}

def _norm(x):
    return re.sub(r"\s+", " ", str(x or "")).strip().upper()

def _ensure_col(df, col, default=""):
    if col not in df.columns:
        df[col] = default
    return col

def _prepare(df):
    df = df.copy()

    col_monto = pick_column(df, EGRESO_MONTO_CANDIDATES)
    col_folio = pick_column(df, ["FOLIO"])
    col_fecha_em = pick_column(df, ["FECHA EMISION", "FECHA_EMISION"])
    col_metodo = pick_column(df, ["METODO PAGO", "METODO DE PAGO"])
    col_forma = pick_column(df, ["FORMA PAGO", "FORMA DE PAGO"])
    col_estado = pick_column(df, ["ESTADO DE PAGO"])
    col_fecha_pago = pick_column(df, ["FECHA DE PAGO"])

    if not col_monto:
        raise ValueError("No se encontró columna de monto")

    if not col_estado:
        col_estado = _ensure_col(df, "ESTADO DE PAGO", "")
    if not col_fecha_pago:
        col_fecha_pago = _ensure_col(df, "FECHA DE PAGO", "")

    df[col_monto] = to_money(df[col_monto]).abs()
    if col_fecha_em:
        df[col_fecha_em] = to_date(df[col_fecha_em])

    df["_USADO_"] = False
    if col_fecha_em:
        df = df.sort_values(col_fecha_em)

    return {
        "df": df,
        "monto": col_monto,
        "folio": col_folio,
        "fecha_em": col_fecha_em,
        "metodo": col_metodo,
        "forma": col_forma,
        "estado": col_estado,
        "fecha_pago": col_fecha_pago,
    }

def conciliar_estado_cuenta_con_movimientos(
    banco: pd.DataFrame,
    ingresos: pd.DataFrame,
    egresos: pd.DataFrame,
    tolerancia: float = 0.01,
):
    banco = banco.copy()

    col_cargo = pick_column(banco, CARGO_COL_CANDIDATES)
    col_fecha_banco = pick_column(banco, FECHA_COL_CANDIDATES)

    if not col_cargo or not col_fecha_banco:
        raise ValueError("Banco: faltan columnas de cargos o fecha")

    col_folio_fact = pick_column(banco, ["FOLIO FACTURA"])
    col_fecha_fact = pick_column(banco, ["FECHA FACTURA"])

    if not col_folio_fact:
        col_folio_fact = _ensure_col(banco, "FOLIO FACTURA", "")
    if not col_fecha_fact:
        col_fecha_fact = _ensure_col(banco, "FECHA FACTURA", "")

    banco[col_cargo] = to_money(banco[col_cargo]).abs()
    banco[col_fecha_banco] = to_date(banco[col_fecha_banco])

    ing = _prepare(ingresos)
    egr = _prepare(egresos)

    def match(pack, monto, fecha_pago):
        df = pack["df"]
        cand = df[
            (~df["_USADO_"]) &
            ((df[pack["monto"]] - monto).abs() <= tolerancia)
        ]

        if cand.empty:
            return None

        row = cand.iloc[0]

        metodo = _norm(row.get(pack["metodo"]))
        forma = _norm(row.get(pack["forma"]))

        if "PPD" in metodo:
            df.at[row.name, pack["estado"]] = "NO PAGADO"
            df.at[row.name, pack["fecha_pago"]] = ""
            df.at[row.name, "_USADO_"] = True
            return None

        if "PUE" in metodo and any(x in forma for x in RESTRICTED_PUE_FORMA):
            df.at[row.name, pack["estado"]] = "NO PAGADO"
            df.at[row.name, pack["fecha_pago"]] = ""
            df.at[row.name, "_USADO_"] = True
            return None

        df.at[row.name, pack["estado"]] = "PAGADO"
        df.at[row.name, pack["fecha_pago"]] = (
            fecha_pago.strftime("%d/%m/%Y") if pd.notna(fecha_pago) else ""
        )
        df.at[row.name, "_USADO_"] = True
        return row, pack

    for i, b in banco.iterrows():
        monto = b[col_cargo]
        if pd.isna(monto) or monto <= 0:
            continue

        fecha_pago = b[col_fecha_banco]

        res = match(egr, monto, fecha_pago)
        if not res:
            res = match(ing, monto, fecha_pago)

        if not res:
            continue

        row, pack = res

        if pack["folio"]:
            banco.at[i, col_folio_fact] = row.get(pack["folio"], "")

        if pack["fecha_em"] and pd.notna(row.get(pack["fecha_em"])):
            banco.at[i, col_fecha_fact] = row[pack["fecha_em"]].strftime("%d/%m/%Y")

    ing["df"].drop(columns=["_USADO_"], inplace=True, errors="ignore")
    egr["df"].drop(columns=["_USADO_"], inplace=True, errors="ignore")

    return banco, ing["df"], egr["df"]
