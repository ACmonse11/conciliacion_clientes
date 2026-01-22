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
            "ABONO", "ABONOS", "CREDITO", "CRÉDITO",
            "DEPOSITO", "DEPÓSITO", "ENTRADA",
            "ENTRADAS", "HABER", "IMPORTE", "MONTO"
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

    if not col_abono or not col_fecha_banco or not col_monto_ing:
        raise ValueError("Faltan columnas necesarias para conciliación de ingresos")

    # =============================
    # Normalizar BANCO
    # =============================
    banco = banco.copy()
    banco[col_abono] = to_money(banco[col_abono])
    banco = banco[banco[col_abono] > 0]  # solo ingresos
    banco[col_fecha_banco] = to_date(banco[col_fecha_banco])

    # 🔒 FLAG DE USO (1 a 1)
    banco["__USADO__"] = False

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
    # CONCILIACIÓN 1 A 1 (CORRECTA)
    # =============================
    for i, ing in ingresos.iterrows():
        monto = ing.get(col_monto_ing)

        if pd.isna(monto):
            continue

        # 🔥 SOLO MOVIMIENTOS NO USADOS
        disponibles = banco[~banco["__USADO__"]]

        # 1️⃣ Buscar por MONTO
        candidates = disponibles[
            (disponibles[col_abono] - float(monto)).abs() <= float(tolerancia)
        ].copy()

        if candidates.empty:
            continue

        # 2️⃣ Filtro por ID_DOCUMENTO (si existe)
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

        # 3️⃣ 🔥 PRIORIDAD ABSOLUTA: MOVIMIENTOS CON FECHA
        with_date = candidates[pd.notna(candidates[col_fecha_banco])]
        if not with_date.empty:
            candidates_final = with_date
        else:
            candidates_final = candidates

        # 4️⃣ SCORING FINAL
        def score_row(row):
            score = 0

            if col_fecha_ing and pd.notna(ing.get(col_fecha_ing)):
                score -= abs((ing[col_fecha_ing] - row[col_fecha_banco]).days)

            if col_conc_ing and col_desc_banco:
                score += fuzz.token_set_ratio(
                    str(ing.get(col_conc_ing, "")),
                    str(row.get(col_desc_banco, ""))
                )

            return score

        candidates_final["__SCORE__"] = candidates_final.apply(score_row, axis=1)
        best = candidates_final.sort_values("__SCORE__", ascending=False).iloc[0]

        # 🔒 MARCAR MOVIMIENTO COMO USADO
        banco.loc[best.name, "__USADO__"] = True

        # ✅ MARCAR INGRESO
        ingresos.at[i, "CONCILIADO_BANCO"] = "SI"
        ingresos.at[i, "ESTADO_INGRESO"] = "COBRADO"

        if pd.notna(best[col_fecha_banco]):
            ingresos.at[i, "FECHA_DE_COBRO"] = best[col_fecha_banco].strftime("%d/%m/%Y")
            ingresos.at[i, "OBSERVACION"] = "Conciliado 1 a 1 con fecha bancaria"
        else:
            ingresos.at[i, "OBSERVACION"] = "Conciliado 1 a 1 sin fecha bancaria"

        conciliados += 1

    resumen = {
        "Ingresos totales": int(len(ingresos)),
        "Cobrados": int(conciliados),
        "No cobrados": int(len(ingresos) - conciliados),
    }

    return ingresos, resumen
