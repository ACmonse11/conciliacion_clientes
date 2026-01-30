import pandas as pd
from .preprocessing import pick_column, to_money, to_date


def conciliar_ingresos_con_abonos(
    ingresos: pd.DataFrame,
    banco: pd.DataFrame,
    tolerancia: float = 0.01,
):
    ingresos = ingresos.copy()
    banco = banco.copy()

    # ===============================
    # COLUMNAS INGRESOS
    # ===============================
    col_total = pick_column(ingresos, ["TOTAL", "IMPORTE", "MONTO"])
    col_estado = pick_column(ingresos, ["ESTADO DE PAGO", "ESTADO_PAGO"])
    col_fecha_pago = pick_column(ingresos, ["FECHA DE PAGO", "FECHA_PAGO"])

    if not col_estado:
        ingresos["ESTADO DE PAGO"] = ""
        col_estado = "ESTADO DE PAGO"

    if not col_fecha_pago:
        ingresos["FECHA DE PAGO"] = ""
        col_fecha_pago = "FECHA DE PAGO"

    # ===============================
    # COLUMNAS BANCO
    # ===============================
    col_abono = pick_column(banco, ["ABONO", "ABONOS"])
    col_fecha_banco = pick_column(banco, ["FECHA"])

    if not col_total or not col_abono:
        return ingresos

    # ===============================
    # NORMALIZAR
    # ===============================
    ingresos[col_total] = to_money(ingresos[col_total]).abs()
    banco[col_abono] = to_money(banco[col_abono]).abs()

    if col_fecha_banco:
        banco[col_fecha_banco] = to_date(banco[col_fecha_banco])

    banco["_USADO_ING_"] = False

    # ===============================
    # MATCH INGRESOS ↔ ABONOS
    # ===============================
    for i, ing in ingresos.iterrows():
        if ing[col_estado] == "PAGADO":
            continue

        total = ing[col_total]
        if pd.isna(total) or total <= 0:
            continue

        matches = banco[
            (~banco["_USADO_ING_"]) &
            ((banco[col_abono] - total).abs() <= tolerancia)
        ]

        if matches.empty:
            continue

        mov = matches.iloc[0]

        ingresos.at[i, col_estado] = "PAGADO"

        if col_fecha_banco and pd.notna(mov.get(col_fecha_banco)):
            ingresos.at[i, col_fecha_pago] = (
                mov[col_fecha_banco].strftime("%d/%m/%Y")
            )

        banco.at[mov.name, "_USADO_ING_"] = True

    banco.drop(columns=["_USADO_ING_"], inplace=True, errors="ignore")
    return ingresos
