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
    # COLUMNAS INGRESOS
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
    # COLUMNAS BANCO
    # ===============================
    col_abono = pick_column(banco, ["ABONO", "ABONOS"])
    col_fecha_banco = pick_column(banco, ["FECHA"])
    col_folio_fact = pick_column(banco, ["FOLIO FACTURA"]) or "FOLIO FACTURA"
    col_fecha_fact = pick_column(banco, ["FECHA FACTURA"]) or "FECHA FACTURA"
    col_folio_cp_out = pick_column(banco, ["FOLIO COMPLEMENTO DE PAGO"]) or "FOLIO COMPLEMENTO DE PAGO"
    col_fecha_cp_out = pick_column(banco, ["FECHA COMPLEMENTO DE PAGO"]) or "FECHA COMPLEMENTO DE PAGO"

    for c in [col_folio_fact, col_fecha_fact, col_folio_cp_out, col_fecha_cp_out]:
        if c not in banco.columns:
            banco[c] = ""

    # ===============================
    # NORMALIZAR DATOS
    # ===============================
    complementos[col_importe_pag] = to_money(complementos[col_importe_pag]).abs()
    banco[col_abono] = to_money(banco[col_abono]).abs()

    if col_fecha_doc:
        complementos[col_fecha_doc] = to_date(complementos[col_fecha_doc])
    if col_fecha_cp:
        complementos[col_fecha_cp] = to_date(complementos[col_fecha_cp])
    if col_fecha_banco:
        banco[col_fecha_banco] = to_date(banco[col_fecha_banco])

    # ===============================
    # PPD REAL: COMPLEMENTOS → BANCO → INGRESOS
    # ===============================
    for _, cp in complementos.iterrows():
        folio = cp[col_folio_doc]
        monto = cp[col_importe_pag]

        if pd.isna(monto) or monto <= 0:
            continue

        # 🔑 VALIDAR QUE EL INGRESO EXISTE
        # ✅ El ingreso puede NO existir en ACUMULADO (por ejemplo folios viejos)
        mask_ing = ingresos_acumulado[col_folio_ing] == folio  # puede ser vacío y está bien


        # 🔑 BUSCAR EN BANCO
        movs = banco[
            (~banco["_USADO_PPD_"]) &
            (banco[col_abono].notna()) &
            ((banco[col_abono] - monto).abs() <= tolerancia)
        ]

        if movs.empty:
            continue

        mov = movs.iloc[0]

        # ---- BANCO
        # ---- BANCO (manejo correcto de NaN)
        if pd.isna(banco.at[mov.name, col_folio_fact]) or banco.at[mov.name, col_folio_fact] == "":
            banco.at[mov.name, col_folio_fact] = folio

        if col_fecha_doc and (
            pd.isna(banco.at[mov.name, col_fecha_fact]) or banco.at[mov.name, col_fecha_fact] == ""
        ):
            banco.at[mov.name, col_fecha_fact] = cp[col_fecha_doc].strftime("%d/%m/%Y")

        banco.at[mov.name, col_folio_cp_out] = cp[col_folio_cp]

        if col_fecha_cp:
            banco.at[mov.name, col_fecha_cp_out] = cp[col_fecha_cp].strftime("%d/%m/%Y")

        banco.at[mov.name, "_USADO_PPD_"] = True

        # ---- INGRESOS
        if mask_ing.any():
            ingresos_acumulado.loc[mask_ing, col_estado] = "PAGADO"
            ingresos_acumulado.loc[mask_ing, col_fecha_pago] = (
                mov[col_fecha_banco].strftime("%d/%m/%Y")
            )

    banco.drop(columns=["_USADO_PPD_"], inplace=True, errors="ignore")
    return banco, ingresos_acumulado

