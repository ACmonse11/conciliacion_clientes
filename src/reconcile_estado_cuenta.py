import pandas as pd

from .preprocessing import pick_column, to_money, to_date
from .config import (
    CARGO_COL_CANDIDATES,
    EGRESO_MONTO_CANDIDATES
)


def conciliar_estado_cuenta_con_egresos(
    banco: pd.DataFrame,
    egresos: pd.DataFrame,
    tolerancia: float = 0.01
):
    # =============================
    # COLUMNAS BANCO
    # =============================
    col_cargo_banco = pick_column(banco, CARGO_COL_CANDIDATES)

    col_folio_factura_banco = pick_column(
        banco,
        ["FOLIO FACTURA", "FOLIO_FACTURA", "FACTURA"]
    )

    col_fecha_factura_banco = pick_column(
        banco,
        ["FECHA FACTURA", "FECHA_FACTURA"]
    )

    # Si no existen, las creamos
    if not col_folio_factura_banco:
        banco["FOLIO FACTURA"] = ""
        col_folio_factura_banco = "FOLIO FACTURA"

    if not col_fecha_factura_banco:
        banco["FECHA FACTURA"] = ""
        col_fecha_factura_banco = "FECHA FACTURA"

    # =============================
    # COLUMNAS EGRESOS
    # =============================
    col_monto_egreso = pick_column(egresos, EGRESO_MONTO_CANDIDATES)

    col_folio_egreso = pick_column(
        egresos,
        ["FOLIO", "FOLIO FACTURA", "FACTURA", "NO_FACTURA"]
    )

    col_fecha_emision_egreso = pick_column(
        egresos,
        ["FECHA EMISION", "FECHA_EMISION", "FECHA FACTURA", "FECHA"]
    )

    if not col_cargo_banco or not col_monto_egreso:
        raise ValueError("No se encontraron columnas necesarias")

    # =============================
    # NORMALIZAR DATOS
    # =============================
    banco = banco.copy()
    banco[col_cargo_banco] = to_money(banco[col_cargo_banco]).abs()

    egresos = egresos.copy()
    egresos[col_monto_egreso] = to_money(egresos[col_monto_egreso]).abs()

    if col_fecha_emision_egreso:
        egresos[col_fecha_emision_egreso] = to_date(
            egresos[col_fecha_emision_egreso]
        )

    # =============================
    # PREPARAR EGRESOS (ordenados)
    # =============================
    egresos["_USADO_"] = False

    if col_fecha_emision_egreso:
        egresos = egresos.sort_values(col_fecha_emision_egreso)

    # =============================
    # CONCILIACIÓN POR MONTO (SECUENCIAL)
    # =============================
    for i, row_banco in banco.iterrows():
        monto_banco = row_banco[col_cargo_banco]

        if pd.isna(monto_banco) or monto_banco <= 0:
            continue

        candidatos = egresos[
            (~egresos["_USADO_"]) &
            ((egresos[col_monto_egreso] - monto_banco).abs() <= tolerancia)
        ]

        if candidatos.empty:
            continue

        # Tomar el PRIMERO disponible
        egreso = candidatos.iloc[0]

        banco.at[i, col_folio_factura_banco] = str(
            egreso.get(col_folio_egreso, "")
        )

        if col_fecha_emision_egreso and pd.notna(
            egreso.get(col_fecha_emision_egreso)
        ):
            banco.at[i, col_fecha_factura_banco] = (
                egreso[col_fecha_emision_egreso].strftime("%d/%m/%Y")
            )

        egresos.at[egreso.name, "_USADO_"] = True

    banco = banco.drop(columns=[], errors="ignore")

    return banco
