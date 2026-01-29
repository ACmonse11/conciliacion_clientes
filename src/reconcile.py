import pandas as pd
from rapidfuzz import fuzz

from .config import (
    CARGO_COL_CANDIDATES,
    FECHA_COL_CANDIDATES,
    DESCRIP_COL_CANDIDATES,
    EGRESO_MONTO_CANDIDATES,
    EGRESO_FECHA_CANDIDATES,
    EGRESO_CONCEPTO_CANDIDATES
)
from .preprocessing import pick_column, to_money, to_date


def conciliar_egresos_vs_banco(
    egresos: pd.DataFrame,
    banco: pd.DataFrame,
    tolerancia: float = 1.0
):
    # =============================
    # COLUMNAS BANCO
    # =============================
    col_cargo = pick_column(banco, CARGO_COL_CANDIDATES)
    col_fecha_banco = pick_column(banco, FECHA_COL_CANDIDATES)
    col_desc_banco = pick_column(banco, DESCRIP_COL_CANDIDATES)

    # =============================
    # COLUMNAS EGRESOS
    # =============================
    col_monto_egr = pick_column(egresos, EGRESO_MONTO_CANDIDATES)
    col_fecha_egr = pick_column(egresos, EGRESO_FECHA_CANDIDATES)
    col_conc_egr = pick_column(egresos, EGRESO_CONCEPTO_CANDIDATES)
    col_forma_pago = pick_column(
        egresos,
        ["FORMA PAGO", "FORMA DE PAGO", "METODO DE PAGO"]
    )

    if not col_cargo or not col_monto_egr:
        raise ValueError("No se encontraron columnas necesarias para egresos")

    # =============================
    # NORMALIZAR BANCO
    # =============================
    banco = banco.copy()
    banco[col_cargo] = to_money(banco[col_cargo]).abs()
    banco[col_fecha_banco] = to_date(banco[col_fecha_banco])

    # =============================
    # NORMALIZAR EGRESOS
    # =============================
    egresos = egresos.copy()
    egresos[col_monto_egr] = to_money(egresos[col_monto_egr]).abs()

    # 🔒 PRESERVAR FECHA ORIGINAL DEL DOCUMENTO
    if col_fecha_egr:
        egresos["FECHA_EMISION"] = egresos[col_fecha_egr].astype(str)
        egresos["_FECHA_EMISION_DT"] = to_date(egresos[col_fecha_egr])
    else:
        egresos["FECHA_EMISION"] = ""
        egresos["_FECHA_EMISION_DT"] = pd.NaT

    # =============================
    # COLUMNAS RESULTADO
    # =============================
    egresos["CONCILIADO_BANCO"] = "NO"
    egresos["ESTADO_EGRESO"] = "NO PAGADO"
    egresos["FECHA_DE_PAGO"] = ""
    egresos["OBSERVACION"] = ""

    # =============================
    # CONCILIACIÓN
    # =============================
    for i, e in egresos.iterrows():
        monto = e.get(col_monto_egr)

        if pd.isna(monto):
            continue

        # ==================================================
        # 1️⃣ EFECTIVO → NO PAGADO
        # ==================================================
        if col_forma_pago:
            forma = str(e.get(col_forma_pago, "")).upper()
            if "EFECTIVO" in forma or "01" in forma:
                egresos.at[i, "ESTADO_EGRESO"] = "NO PAGADO"
                egresos.at[i, "OBSERVACION"] = "Forma de pago EFECTIVO"
                continue

        # ==================================================
        # 2️⃣ BUSCAR EN BANCO
        # ==================================================
        candidatos = banco[
            (banco[col_cargo] - monto).abs() <= tolerancia
        ].copy()

        if candidatos.empty:
            continue

        candidatos = candidatos[pd.notna(candidatos[col_fecha_banco])]
        if candidatos.empty:
            continue

        # -----------------------------
        # Scoring
        # -----------------------------
        def score_row(row):
            score = 0.0

            score += 1000  # fecha válida

            if col_fecha_egr and pd.notna(e["_FECHA_EMISION_DT"]):
                diff_days = abs(
                    (e["_FECHA_EMISION_DT"] - row[col_fecha_banco]).days
                )
                score += max(0, 300 - diff_days)

            if col_conc_egr and col_desc_banco:
                score += fuzz.token_set_ratio(
                    str(e.get(col_conc_egr, "")),
                    str(row.get(col_desc_banco, ""))
                )
            return score

        candidatos["__SCORE__"] = candidatos.apply(score_row, axis=1)
        best = candidatos.sort_values("__SCORE__", ascending=False).iloc[0]

        egresos.at[i, "CONCILIADO_BANCO"] = "SI"
        egresos.at[i, "ESTADO_EGRESO"] = "PAGADO"
        egresos.at[i, "FECHA_DE_PAGO"] = (
            best[col_fecha_banco].strftime("%d/%m/%Y")
            if pd.notna(best[col_fecha_banco])
            else ""
        )
        egresos.at[i, "OBSERVACION"] = "Conciliado con estado de cuenta"

    # =============================
    # RESUMEN
    # =============================
    resumen = {
        "Egresos totales": int(len(egresos)),
        "Pagados": int((egresos["ESTADO_EGRESO"] == "PAGADO").sum()),
        "No pagados": int((egresos["ESTADO_EGRESO"] == "NO PAGADO").sum())
    }

    # Limpieza columna técnica
    egresos.drop(columns=["_FECHA_EMISION_DT"], inplace=True, errors="ignore")

    return egresos, resumen
