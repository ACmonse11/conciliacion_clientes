import pandas as pd

from .config import (
    EGRESO_MONTO_CANDIDATES,
    INGRESO_ID_CANDIDATES
)
from .preprocessing import pick_column, to_money, to_date


def conciliar_ingresos_vs_banco(
    ingresos: pd.DataFrame,
    banco: pd.DataFrame,
    tolerancia: float = 1.0
):
    # =============================
    # COLUMNAS BANCO
    # =============================
    col_abono = pick_column(
        banco,
        [
            "ABONO", "ABONOS", "CREDITO", "CRÉDITO",
            "DEPOSITO", "DEPÓSITO", "IMPORTE", "MONTO"
        ]
    )
    col_folio_banco = pick_column(
        banco,
        ["FOLIO", "FOLIO FACTURA", "FACTURA", "NO FACTURA"]
    )
    col_fecha_banco = pick_column(
        banco,
        ["FECHA FACTURA", "FECHA"]
    )

    # =============================
    # COLUMNAS INGRESOS
    # =============================
    col_monto_ing = pick_column(ingresos, EGRESO_MONTO_CANDIDATES)
    col_folio_ing = pick_column(ingresos, INGRESO_ID_CANDIDATES)
    col_metodo_pago = pick_column(
        ingresos,
        ["METODO PAGO", "METODO DE PAGO", "FORMA DE PAGO"]
    )
    col_fecha_ingreso = pick_column(
        ingresos,
        ["FECHA EMISION", "FECHA_EMISION", "FECHA FACTURA", "FECHA"]
    )

    if not col_abono or not col_folio_ing or not col_monto_ing:
        raise ValueError("No se encontraron columnas clave para conciliación")

    # =============================
    # NORMALIZAR BANCO
    # =============================
    banco = banco.copy()
    banco[col_abono] = to_money(banco[col_abono]).abs()

    if col_folio_banco:
        banco[col_folio_banco] = banco[col_folio_banco].astype(str).str.strip()

    if col_fecha_banco:
        banco[col_fecha_banco] = to_date(banco[col_fecha_banco])

    banco["__USADO__"] = False

    # =============================
    # NORMALIZAR INGRESOS
    # =============================
    ingresos = ingresos.copy()
    ingresos[col_monto_ing] = to_money(ingresos[col_monto_ing]).abs()
    ingresos[col_folio_ing] = ingresos[col_folio_ing].astype(str).str.strip()

    if col_fecha_ingreso:
        ingresos[col_fecha_ingreso] = to_date(ingresos[col_fecha_ingreso])

    # =============================
    # COLUMNAS RESULTADO
    # =============================
    ingresos["CONCILIADO_BANCO"] = "NO"
    ingresos["ESTADO_INGRESO"] = "NO PAGADO"
    ingresos["FECHA_DE_PAGO"] = ""
    ingresos["OBSERVACION"] = ""

    # =============================
    # CONCILIACIÓN
    # =============================
    for i, ing in ingresos.iterrows():
        folio = ing.get(col_folio_ing)
        monto = ing.get(col_monto_ing)

        if pd.isna(folio) or pd.isna(monto):
            continue

        # ==================================================
        # 1️⃣ PUE → PAGADO AUTOMÁTICO (fecha = emisión)
        # ==================================================
        if col_metodo_pago:
            metodo = str(ing.get(col_metodo_pago, "")).upper()

            if "PUE" in metodo:
                ingresos.at[i, "CONCILIADO_BANCO"] = "SI"
                ingresos.at[i, "ESTADO_INGRESO"] = "PAGADO"
                ingresos.at[i, "OBSERVACION"] = "Pago PUE - Una sola exhibición"

                if col_fecha_ingreso and pd.notna(ing.get(col_fecha_ingreso)):
                    fecha = ing.get(col_fecha_ingreso)
                    if pd.notna(fecha):
                        ingresos.at[i, "FECHA_DE_PAGO"] = fecha.strftime("%d/%m/%Y")

                continue

        # ==================================================
        # 2️⃣ FOLIO + MONTO (BANCO)
        # ==================================================
        if not col_folio_banco:
            continue

        candidatos = banco[
            (~banco["__USADO__"]) &
            (banco[col_folio_banco] == str(folio)) &
            ((banco[col_abono] - monto).abs() <= tolerancia)
        ]

        if candidatos.empty:
            continue

        best = candidatos.iloc[0]
        banco.loc[best.name, "__USADO__"] = True

        ingresos.at[i, "CONCILIADO_BANCO"] = "SI"
        ingresos.at[i, "ESTADO_INGRESO"] = "PAGADO"
        ingresos.at[i, "OBSERVACION"] = "Conciliado por FOLIO + MONTO"

        if col_fecha_banco and pd.notna(best.get(col_fecha_banco)):
            ingresos.at[i, "FECHA_DE_PAGO"] = best[col_fecha_banco].strftime("%d/%m/%Y")

    # =============================
    # RESUMEN
    # =============================
    resumen = {
        "Ingresos totales": int(len(ingresos)),
        "Pagados": int((ingresos["ESTADO_INGRESO"] == "PAGADO").sum()),
        "No pagados": int((ingresos["ESTADO_INGRESO"] == "NO PAGADO").sum())
    }

    return ingresos, resumen
