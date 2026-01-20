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
    # Detectar columnas BANCO
    # =============================
    col_abono = pick_column(
        banco,
        [
            "ABONO",
            "ABONOS",
            "CREDITO",
            "CRÉDITO",
            "DEPOSITO",
            "DEPÓSITO",
            "ENTRADA",
            "ENTRADAS",
            "HABER",
            "IMPORTE",
            "MONTO"
        ]
    )

    col_fecha_banco = pick_column(banco, FECHA_COL_CANDIDATES)
    col_desc_banco = pick_column(banco, DESCRIP_COL_CANDIDATES)

    # =============================
    # Detectar columnas INGRESOS
    # =============================
    col_monto_ing = pick_column(ingresos, EGRESO_MONTO_CANDIDATES)
    col_fecha_ing = pick_column(ingresos, EGRESO_FECHA_CANDIDATES)
    col_conc_ing = pick_column(ingresos, EGRESO_CONCEPTO_CANDIDATES)
    col_id_ing = pick_column(ingresos, INGRESO_ID_CANDIDATES)

    if not col_abono:
        raise ValueError("No se encontró columna de ABONO / IMPORTE en banco")
    if not col_fecha_banco:
        raise ValueError("No se encontró columna de FECHA en banco")
    if not col_monto_ing:
        raise ValueError("No se encontró columna de MONTO en ingresos")

    # =============================
    # Normalizar BANCO
    # =============================
    banco = banco.copy()
    banco[col_abono] = to_money(banco[col_abono])

    # 👉 SOLO INGRESOS (POSITIVOS)
    banco = banco[banco[col_abono] > 0]

    banco[col_fecha_banco] = to_date(banco[col_fecha_banco])

    # =============================
    # Normalizar INGRESOS
    # =============================
    ingresos = ingresos.copy()
    ingresos[col_monto_ing] = to_money(ingresos[col_monto_ing]).abs()

    if col_fecha_ing:
        ingresos[col_fecha_ing] = to_date(ingresos[col_fecha_ing])

    # =============================
    # Columnas de salida
    # =============================
    ingresos["CONCILIADO_BANCO"] = "NO"
    ingresos["ESTADO_INGRESO"] = "NO COBRADO"
    ingresos["FECHA_DE_COBRO"] = ""
    ingresos["OBSERVACION"] = ""

    conciliados = 0

    # =============================
    # Conciliación
    # =============================
    for i, ing in ingresos.iterrows():
        monto = ing.get(col_monto_ing)

        if pd.isna(monto):
            ingresos.at[i, "OBSERVACION"] = "Ingreso sin monto"
            continue

        # -----------------------------
        # 1️⃣ Buscar por MONTO
        # -----------------------------
        candidates = banco[
            (banco[col_abono] - float(monto)).abs() <= float(tolerancia)
        ].copy()

        if candidates.empty:
            ingresos.at[i, "OBSERVACION"] = "No se encontró abono en banco"
            continue

        # -----------------------------
        # 2️⃣ FILTRO POR ID_DOCUMENTO
        # -----------------------------
        if col_id_ing:
            id_doc = str(ing.get(col_id_ing, "")).strip()

            if id_doc:
                candidates_id = candidates[
                    candidates[col_desc_banco]
                    .astype(str)
                    .str.contains(id_doc, case=False, na=False)
                ]

                if not candidates_id.empty:
                    candidates = candidates_id

        # 👉 YA HAY MONTO (+ ID si aplica) → COBRADO
        ingresos.at[i, "CONCILIADO_BANCO"] = "SI"
        ingresos.at[i, "ESTADO_INGRESO"] = "COBRADO"

        # -----------------------------
        # Intentar fecha
        # -----------------------------
        with_date = candidates[pd.notna(candidates[col_fecha_banco])]

        if with_date.empty:
            ingresos.at[i, "OBSERVACION"] = "Cobro identificado (monto/ID), sin fecha"
            conciliados += 1
            continue

        # -----------------------------
        # Scoring
        # -----------------------------
        def score_row(row):
            score = 0.0

            if col_fecha_ing and pd.notna(ing.get(col_fecha_ing)):
                diff_days = abs((ing[col_fecha_ing] - row[col_fecha_banco]).days)
                score += max(0, 300 - diff_days)

            if col_conc_ing and col_desc_banco:
                sim = fuzz.token_set_ratio(
                    str(ing.get(col_conc_ing, "")),
                    str(row.get(col_desc_banco, ""))
                )
                score += sim

            return score

        with_date["__SCORE__"] = with_date.apply(score_row, axis=1)
        best = with_date.sort_values("__SCORE__", ascending=False).iloc[0]

        fecha_cobro = best[col_fecha_banco]

        if pd.notna(fecha_cobro):
            ingresos.at[i, "FECHA_DE_COBRO"] = fecha_cobro.strftime("%d/%m/%Y")
            ingresos.at[i, "OBSERVACION"] = "Conciliado por ID_DOCUMENTO + monto"
        else:
            ingresos.at[i, "OBSERVACION"] = "Cobro identificado, sin fecha"

        conciliados += 1

    resumen = {
        "Ingresos totales": int(len(ingresos)),
        "Cobrados": int(conciliados),
        "No encontrados en banco": int(len(ingresos) - conciliados),
        "Columna ID ingreso": col_id_ing,
        "Columna abono banco": col_abono,
    }

    return ingresos, resumen
