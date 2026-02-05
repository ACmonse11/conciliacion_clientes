import re
import pandas as pd
from .preprocessing import pick_column, to_money, to_date
from .config import CARGO_COL_CANDIDATES, FECHA_COL_CANDIDATES, EGRESO_MONTO_CANDIDATES
from .reconcile import conciliar_egresos_vs_banco


def conciliar_estado_cuenta_con_movimientos(
    banco: pd.DataFrame,
    ingresos: pd.DataFrame,
    egresos: pd.DataFrame,
    tolerancia: float = 0.01,
):
    banco = banco.copy()
    banco.columns = banco.columns.astype(str).str.upper().str.strip()

    col_cargo = pick_column(banco, CARGO_COL_CANDIDATES)
    col_abono = pick_column(banco, ["ABONO", "ABONOS"])
    col_fecha = pick_column(banco, FECHA_COL_CANDIDATES)

    if col_cargo:
        banco[col_cargo] = to_money(banco[col_cargo]).abs()
    if col_abono:
        banco[col_abono] = to_money(banco[col_abono]).abs()

    banco[col_fecha] = to_date(banco[col_fecha])

    # 🔑 Conciliar egresos (incluye EFECTIVO → PAGADO OTRO)
    egresos_out, _ = conciliar_egresos_vs_banco(
        egresos=egresos,
        banco=banco,
        tolerancia=tolerancia
    )

    return banco, ingresos, egresos_out
