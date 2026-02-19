import pandas as pd
import numpy as np
from .preprocessing import pick_column, to_money, to_date


def conciliar_ppd_desde_complementos(
    complementos: pd.DataFrame,
    ingresos_acumulado: pd.DataFrame,
    banco: pd.DataFrame,
    tolerancia: float = 0.01,
    tipo_movimiento: str = "ABONO",  # "ABONO" para ingresos, "CARGO" para egresos
):
    banco = banco.copy()
    complementos = complementos.copy()
    ingresos_acumulado = ingresos_acumulado.copy()

    # ===============================
    # NORMALIZAR COLUMNAS
    # ===============================
    for df in (banco, complementos, ingresos_acumulado):
        df.columns = df.columns.astype(str).str.upper().str.strip()

    banco["_USADO_PPD_"] = False

    # ===============================
    # COLUMNAS COMPLEMENTOS
    # ===============================
    col_folio_doc = pick_column(complementos, ["FOLIO DOCUMENTO", "FOLIO DOC", "FOLIO"])
    col_fecha_doc = pick_column(
        complementos,
        ["FECHA EMISION (DOC)", "FECHA EMISION(DOC)", "FECHA EMISION DOC", "FECHA DOC"]
    )
    col_importe_pag = pick_column(complementos, ["IMPORTE PAGADO"])
    col_folio_cp = pick_column(complementos, ["FOLIO", "FOLIO COMPLEMENTO DE PAGO"])
    col_fecha_cp = pick_column(complementos, ["FECHA EMISION", "FECHA COMPLEMENTO DE PAGO"])

    # ===============================
    # COLUMNAS INGRESOS / EGRESOS
    # ===============================
    col_folio_ing = pick_column(ingresos_acumulado, ["FOLIO"])
    col_estado = pick_column(ingresos_acumulado, ["ESTADO DE PAGO"])
    col_fecha_pago = pick_column(ingresos_acumulado, ["FECHA DE PAGO"])

    if not col_estado:
        ingresos_acumulado["ESTADO DE PAGO"] = ""
        col_estado = "ESTADO DE PAGO"

    if not col_fecha_pago:
        ingresos_acumulado["FECHA DE PAGO"] = ""
        col_fecha_pago = "FECHA DE PAGO"

    # ===============================
    # COLUMNAS BANCO (DINÁMICO)
    # ===============================
    if tipo_movimiento.upper() == "ABONO":
        col_mov = pick_column(banco, ["ABONO", "ABONOS"])
    else:
        col_mov = pick_column(banco, ["CARGO", "CARGOS"])

    col_fecha_banco = pick_column(banco, ["FECHA"])
    col_folio_fact = pick_column(banco, ["FOLIO FACTURA"]) or "FOLIO FACTURA"
    col_fecha_fact = pick_column(banco, ["FECHA FACTURA"]) or "FECHA FACTURA"
    col_folio_cp_out = pick_column(banco, ["FOLIO COMPLEMENTO DE PAGO"]) or "FOLIO COMPLEMENTO DE PAGO"
    col_fecha_cp_out = pick_column(banco, ["FECHA COMPLEMENTO DE PAGO", "FCHA COMPLEMENTO DE PAGO"]) or "FECHA COMPLEMENTO DE PAGO"

    #* Reutilizar columnas existentes (evita duplicados)
    for c in [col_folio_fact, col_fecha_fact, col_folio_cp_out, col_fecha_cp_out]:
        if c not in banco.columns:
            banco[c] = ""

    # ===============================
    # NORMALIZAR DATOS
    # ===============================
    complementos[col_importe_pag] = to_money(complementos[col_importe_pag]).abs()
    banco[col_mov] = to_money(banco[col_mov]).abs()

    """ if col_fecha_doc:
        complementos[col_fecha_doc] = to_date(complementos[col_fecha_doc]) """
    if col_fecha_cp:
        complementos[col_fecha_cp] = to_date(complementos[col_fecha_cp])
    if col_fecha_banco:
        banco[col_fecha_banco] = to_date(banco[col_fecha_banco])

    # ===============================
    # PPD REAL
    # ===============================
    for _, cp in complementos.iterrows():

        folio = cp[col_folio_doc]
        monto = cp[col_importe_pag]

        if pd.isna(monto) or monto <= 0:
            continue

        mask_ing = ingresos_acumulado[col_folio_ing] == folio

        # 🔎 Buscar movimiento en banco
        movs = banco[
            (banco[col_mov].notna()) &
            (
                np.isclose(
                    banco[col_mov],
                    monto,
                    atol=tolerancia
                )
            ) &
            (~banco["_USADO_PPD_"])
        ]

        if movs.empty:
            continue

        mov = movs.iloc[0]

        # ===============================
        # ACTUALIZAR BANCO
        # ===============================
        if pd.isna(banco.at[mov.name, col_folio_fact]) or banco.at[mov.name, col_folio_fact] == "":
            banco.at[mov.name, col_folio_fact] = folio

        if col_fecha_doc:
            fecha_val = cp.get(col_fecha_doc)
            if pd.notna(fecha_val) and str(fecha_val).strip() != "":
                banco.at[mov.name, col_fecha_fact] = str(fecha_val)

        banco.at[mov.name, col_folio_cp_out] = cp[col_folio_cp]

        if col_fecha_cp and pd.notna(cp[col_fecha_cp]):
            banco.at[mov.name, col_fecha_cp_out] = cp[col_fecha_cp].strftime("%d/%m/%Y")

        banco.at[mov.name, "_USADO_PPD_"] = True

        # ===============================
        # ACTUALIZAR INGRESOS / EGRESOS
        # ===============================
        if mask_ing.any():
            ingresos_acumulado.loc[mask_ing, col_estado] = "PAGADO"

            if col_fecha_banco and pd.notna(mov[col_fecha_banco]):
                ingresos_acumulado.loc[mask_ing, col_fecha_pago] = (
                    mov[col_fecha_banco].strftime("%d/%m/%Y")
                )

    banco.drop(columns=["_USADO_PPD_"], inplace=True, errors="ignore")

    return banco, ingresos_acumulado

