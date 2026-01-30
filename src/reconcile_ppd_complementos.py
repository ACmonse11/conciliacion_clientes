import pandas as pd
from .preprocessing import pick_column, to_money, to_date


def conciliar_ppd_desde_complementos(
    complementos: pd.DataFrame,
    ingresos_acumulado: pd.DataFrame,
    banco: pd.DataFrame,
    tolerancia: float = 0.01,
):
    banco = banco.copy()
    complementos = complementos.copy()
    ingresos_acumulado = ingresos_acumulado.copy()

    # =====================================================
    # COLUMNAS COMPLEMENTOS
    # =====================================================
    col_folio_doc = pick_column(
        complementos,
        ["FOLIO DOCUMENTO", "FOLIO DOC", "FOLIO"]
    )
    col_saldo_ant = pick_column(complementos, ["SALDO ANTERIOR"])
    col_importe_pag = pick_column(complementos, ["IMPORTE PAGADO"])
    col_folio_cp = pick_column(
        complementos,
        ["FOLIO", "FOLIO COMPLEMENTO DE PAGO", "FOLIO COMPLEMENTO"]
    )
    col_fecha_cp = pick_column(
        complementos,
        ["FECHA EMISION", "FECHA COMPLEMENTO DE PAGO", "FECHA COMPLEMENTO"]
    )

    if not col_folio_doc or not col_saldo_ant or not col_importe_pag:
        raise ValueError("Faltan columnas clave en la hoja COMPLEMENTOS")

    # =====================================================
    # COLUMNAS INGRESOS (ACUMULADO)
    # =====================================================
    col_folio_ing = pick_column(ingresos_acumulado, ["FOLIO"])
    col_estado_pago = pick_column(
        ingresos_acumulado,
        ["ESTADO DE PAGO", "ESTADO_PAGO"]
    )
    col_fecha_pago = pick_column(
        ingresos_acumulado,
        ["FECHA DE PAGO", "FECHA_PAGO"]
    )

    if not col_estado_pago:
        ingresos_acumulado["ESTADO DE PAGO"] = ""
        col_estado_pago = "ESTADO DE PAGO"

    if not col_fecha_pago:
        ingresos_acumulado["FECHA DE PAGO"] = ""
        col_fecha_pago = "FECHA DE PAGO"

    # =====================================================
    # COLUMNAS BANCO
    # =====================================================
    col_abono = pick_column(banco, ["ABONO", "ABONOS"])
    col_fecha_banco = pick_column(banco, ["FECHA"])

    if not col_abono:
        raise ValueError("No se encontró columna ABONOS en banco")

    col_folio_fact = pick_column(banco, ["FOLIO FACTURA"])
    if not col_folio_fact:
        banco["FOLIO FACTURA"] = ""
        col_folio_fact = "FOLIO FACTURA"

    col_fecha_fact = pick_column(banco, ["FECHA FACTURA"])
    if not col_fecha_fact:
        banco["FECHA FACTURA"] = ""
        col_fecha_fact = "FECHA FACTURA"

    col_folio_cp_out = pick_column(banco, ["FOLIO COMPLEMENTO DE PAGO"])
    if not col_folio_cp_out:
        banco["FOLIO COMPLEMENTO DE PAGO"] = ""
        col_folio_cp_out = "FOLIO COMPLEMENTO DE PAGO"

    col_fecha_cp_out = pick_column(banco, ["FECHA COMPLEMENTO DE PAGO"])
    if not col_fecha_cp_out:
        banco["FECHA COMPLEMENTO DE PAGO"] = ""
        col_fecha_cp_out = "FECHA COMPLEMENTO DE PAGO"

    # =====================================================
    # NORMALIZAR
    # =====================================================
    complementos[col_saldo_ant] = to_money(complementos[col_saldo_ant]).abs()
    complementos[col_importe_pag] = to_money(complementos[col_importe_pag]).abs()
    banco[col_abono] = to_money(banco[col_abono]).abs()

    if col_fecha_cp:
        complementos[col_fecha_cp] = to_date(complementos[col_fecha_cp])

    if col_fecha_banco:
        banco[col_fecha_banco] = to_date(banco[col_fecha_banco])

    # =====================================================
    # AGRUPAR COMPLEMENTOS POR FACTURA
    # =====================================================
    grouped = (
        complementos
        .groupby(col_folio_doc, dropna=False)
        .agg({
            col_saldo_ant: "sum",
            col_importe_pag: "sum",
            col_folio_cp: lambda x: ", ".join(x.astype(str)),
            col_fecha_cp: "max",
        })
        .reset_index()
    )

    # =====================================================
    # MATCH CONTRA BANCO + MARCAR INGRESOS PAGADOS
    # =====================================================
    for _, row in grouped.iterrows():
        total_pagado = row[col_importe_pag]
        if pd.isna(total_pagado) or total_pagado <= 0:
            continue

        movimientos = banco[
            (banco[col_abono].notna()) &
            ((banco[col_abono] - total_pagado).abs() <= tolerancia)
        ]

        if movimientos.empty:
            continue

        mov = movimientos.iloc[0]

        # -----------------------------
        # ESCRIBIR EN BANCO
        # -----------------------------
        banco.at[mov.name, col_folio_fact] = row[col_folio_doc]
        banco.at[mov.name, col_folio_cp_out] = row[col_folio_cp]

        if col_fecha_cp and pd.notna(row[col_fecha_cp]):
            banco.at[mov.name, col_fecha_cp_out] = (
                row[col_fecha_cp].strftime("%d/%m/%Y")
            )

        # -----------------------------
        # MARCAR INGRESO COMO PAGADO
        # -----------------------------
        mask_ing = ingresos_acumulado[col_folio_ing] == row[col_folio_doc]
        ingresos_acumulado.loc[mask_ing, col_estado_pago] = "PAGADO"

        if col_fecha_banco and pd.notna(mov.get(col_fecha_banco)):
            ingresos_acumulado.loc[
                mask_ing, col_fecha_pago
            ] = mov[col_fecha_banco].strftime("%d/%m/%Y")

    return banco, ingresos_acumulado
