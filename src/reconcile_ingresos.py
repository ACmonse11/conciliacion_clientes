import pandas as pd
from rapidfuzz import fuzz

from .config import (
    FECHA_COL_CANDIDATES,
    DESCRIP_COL_CANDIDATES,
    EGRESO_MONTO_CANDIDATES,
    EGRESO_FECHA_CANDIDATES,
    EGRESO_CONCEPTO_CANDIDATES,
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
            "DEPOSITO", "DEPÓSITO", "ENTRADA",
            "ENTRADAS", "HABER", "IMPORTE", "MONTO"
        ]
    )
    col_fecha_banco = pick_column(banco, FECHA_COL_CANDIDATES)
    col_desc_banco = pick_column(banco, DESCRIP_COL_CANDIDATES)

    # =============================
    # COLUMNAS INGRESOS
    # =============================
    col_monto_ing = pick_column(ingresos, EGRESO_MONTO_CANDIDATES)
    col_fecha_ing = pick_column(ingresos, EGRESO_FECHA_CANDIDATES)
    col_conc_ing = pick_column(ingresos, EGRESO_CONCEPTO_CANDIDATES)
    col_id_ing = pick_column(ingresos, INGRESO_ID_CANDIDATES)

    col_metodo_pago = pick_column(
        ingresos,
        ["METODO", "METODO PAGO", "METODO_PAGO", "FORMA", "FORMA PAGO", "PAGO"]
    )

    if not col_abono or not col_fecha_banco or not col_monto_ing:
        raise ValueError("Faltan columnas necesarias para conciliación")

    # =============================
    # NORMALIZAR BANCO
    # =============================
    banco = banco.copy()
    banco[col_abono] = to_money(banco[col_abono])
    banco = banco[banco[col_abono] > 0]
    banco[col_fecha_banco] = to_date(banco[col_fecha_banco]).dt.normalize()
    banco["__USADO__"] = False

    # =============================
    # NORMALIZAR INGRESOS
    # =============================
    ingresos = ingresos.copy()
    ingresos[col_monto_ing] = to_money(ingresos[col_monto_ing]).abs()

    if col_fecha_ing:
        ingresos["_FECHA_EMISION_DT"] = to_date(ingresos[col_fecha_ing])
    else:
        ingresos["_FECHA_EMISION_DT"] = pd.NaT

    # =============================
    # COLUMNAS SALIDA (FORZADAS)
    # =============================
    ingresos["CONCILIADO_BANCO"] = ""
    ingresos["ESTADO_INGRESO"] = ""
    ingresos["FECHA_DE_COBRO"] = ""
    ingresos["OBSERVACION"] = ""

    # =============================
    # 🔒 LIMPIEZA GLOBAL PPD (CLAVE)
    # =============================
    if col_metodo_pago:
        mask_ppd = (
            ingresos[col_metodo_pago]
            .astype(str)
            .str.upper()
            .str.startswith("PPD")
        )
        ingresos.loc[mask_ppd, [
            "CONCILIADO_BANCO",
            "ESTADO_INGRESO",
            "FECHA_DE_COBRO",
            "OBSERVACION"
        ]] = ""

    conciliados = 0

    # =============================
    # CONCILIACIÓN (SOLO PUE)
    # =============================
    for i, ing in ingresos.iterrows():
        metodo_pago = ""
        if col_metodo_pago:
            metodo_pago = str(ing.get(col_metodo_pago, "")).upper().strip()

        # 🚫 PPD → JAMÁS SE CONCILIA
        if metodo_pago.startswith("PPD"):
            continue

        monto = ing.get(col_monto_ing)
        if pd.isna(monto):
            continue

        disponibles = banco[~banco["__USADO__"]]

        candidates = disponibles[
            (disponibles[col_abono] - float(monto)).abs() <= float(tolerancia)
        ].copy()

        if candidates.empty:
            continue

        if col_id_ing:
            id_doc = str(ing.get(col_id_ing, "")).strip()
            if id_doc:
                by_id = candidates[
                    candidates[col_desc_banco]
                    .astype(str)
                    .str.contains(id_doc, case=False, na=False)
                ]
                if not by_id.empty:
                    candidates = by_id

        def score_row(row):
            score = 0
            if pd.notna(ing["_FECHA_EMISION_DT"]):
                diff = abs(
                    (ing["_FECHA_EMISION_DT"].normalize() - row[col_fecha_banco]).days
                )
                score += max(0, 300 - diff)

            if col_conc_ing and col_desc_banco:
                score += fuzz.token_set_ratio(
                    str(ing.get(col_conc_ing, "")),
                    str(row.get(col_desc_banco, ""))
                )
            return score

        candidates["__SCORE__"] = candidates.apply(score_row, axis=1)
        best = candidates.sort_values("__SCORE__", ascending=False).iloc[0]

        # ✅ SOLO PUE SE MARCA PAGADO
        banco.loc[best.name, "__USADO__"] = True
        ingresos.at[i, "CONCILIADO_BANCO"] = "SI"
        ingresos.at[i, "ESTADO_INGRESO"] = "PAGADO"
        ingresos.at[i, "FECHA_DE_COBRO"] = best[col_fecha_banco].strftime("%d/%m/%Y")
        ingresos.at[i, "OBSERVACION"] = "PUE - Conciliado con banco"
        conciliados += 1

    resumen = {
        "Ingresos totales": int(len(ingresos)),
        "Cobrados": int(conciliados),
        "No cobrados": int(len(ingresos) - conciliados),
    }

    ingresos.drop(columns=["_FECHA_EMISION_DT"], inplace=True, errors="ignore")
    return ingresos, resumen
