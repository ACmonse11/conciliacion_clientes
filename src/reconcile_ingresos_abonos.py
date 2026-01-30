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
    col_folio_ing = pick_column(ingresos, ["FOLIO"])
    col_fecha_em = pick_column(ingresos, ["FECHA EMISION", "FECHA_EMISION"])

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
    col_cargo = pick_column(banco, ["CARGO", "CARGOS"])
    col_fecha_banco = pick_column(banco, ["FECHA"])

    col_folio_fact = pick_column(banco, ["FOLIO FACTURA"])
    if not col_folio_fact:
        banco["FOLIO FACTURA"] = ""
        col_folio_fact = "FOLIO FACTURA"

    col_fecha_fact = pick_column(banco, ["FECHA FACTURA"])
    if not col_fecha_fact:
        banco["FECHA FACTURA"] = ""
        col_fecha_fact = "FECHA FACTURA"

    if not col_total or not col_fecha_banco:
        return ingresos

    # ===============================
    # NORMALIZAR
    # ===============================
    ingresos[col_total] = to_money(ingresos[col_total]).abs()

    if col_fecha_em:
        ingresos[col_fecha_em] = to_date(ingresos[col_fecha_em])

    if col_abono:
        banco[col_abono] = to_money(banco[col_abono]).abs()

    if col_cargo:
        banco[col_cargo] = to_money(banco[col_cargo]).abs()

    banco[col_fecha_banco] = to_date(banco[col_fecha_banco])
    banco["_USADO_ING_"] = False

    # ===============================
    # MATCH INGRESOS ↔ BANCO
    # ===============================
    for i, ing in ingresos.iterrows():
        if ing[col_estado] == "PAGADO":
            continue

        total = ing[col_total]
        if pd.isna(total) or total <= 0:
            continue

        movimientos = pd.DataFrame()

        # Buscar en ABONOS
        if col_abono:
            movimientos = banco[
                (~banco["_USADO_ING_"]) &
                ((banco[col_abono] - total).abs() <= tolerancia)
            ]

        # Si no encontró, buscar en CARGOS
        if movimientos.empty and col_cargo:
            movimientos = banco[
                (~banco["_USADO_ING_"]) &
                ((banco[col_cargo] - total).abs() <= tolerancia)
            ]

        if movimientos.empty:
            continue

        mov = movimientos.iloc[0]

        # ===============================
        # MARCAR INGRESO COMO PAGADO
        # ===============================
        ingresos.at[i, col_estado] = "PAGADO"
        ingresos.at[i, col_fecha_pago] = (
            mov[col_fecha_banco].strftime("%d/%m/%Y")
        )

        # ===============================
        # ESCRIBIR EN BANCO
        # ===============================
        banco.at[mov.name, col_folio_fact] = ing.get(col_folio_ing, "")

        if col_fecha_em and pd.notna(ing.get(col_fecha_em)):
            banco.at[mov.name, col_fecha_fact] = (
                ing[col_fecha_em].strftime("%d/%m/%Y")
            )

        banco.at[mov.name, "_USADO_ING_"] = True

    banco.drop(columns=["_USADO_ING_"], inplace=True, errors="ignore")
    return ingresos
