import pandas as pd
from rapidfuzz import fuzz

from .config import (
    CARGO_COL_CANDIDATES, FECHA_COL_CANDIDATES, DESCRIP_COL_CANDIDATES,
    EGRESO_MONTO_CANDIDATES, EGRESO_FECHA_CANDIDATES, EGRESO_CONCEPTO_CANDIDATES
)
from .preprocessing import pick_column, to_money, to_date

def conciliar_egresos_vs_banco(egresos: pd.DataFrame, banco: pd.DataFrame, tolerancia: float = 1.0):

    # -----------------------------
    # Detectar columnas
    # -----------------------------
    col_cargo = pick_column(banco, CARGO_COL_CANDIDATES)
    col_fecha_banco = pick_column(banco, FECHA_COL_CANDIDATES)
    col_desc_banco = pick_column(banco, DESCRIP_COL_CANDIDATES)

    col_monto_egr = pick_column(egresos, EGRESO_MONTO_CANDIDATES)
    col_fecha_egr = pick_column(egresos, EGRESO_FECHA_CANDIDATES)
    col_conc_egr = pick_column(egresos, EGRESO_CONCEPTO_CANDIDATES)

    if not col_cargo:
        raise ValueError("No se encontró columna de CARGO/DEBITO en banco")
    if not col_fecha_banco:
        raise ValueError("No se encontró columna de FECHA en banco")
    if not col_monto_egr:
        raise ValueError("No se encontró columna de MONTO/TOTAL en egresos")

    # -----------------------------
    # Normalizar BANCO
    # -----------------------------
    banco = banco.copy()
    banco[col_cargo] = to_money(banco[col_cargo]).abs()
    banco[col_fecha_banco] = to_date(banco[col_fecha_banco])

    # -----------------------------
    # Normalizar EGRESOS
    # -----------------------------
    egresos = egresos.copy()
    egresos[col_monto_egr] = to_money(egresos[col_monto_egr]).abs()

    if col_fecha_egr:
        egresos[col_fecha_egr] = to_date(egresos[col_fecha_egr])

    # -----------------------------
    # Columnas resultado
    # -----------------------------
    egresos["CONCILIADO_BANCO"] = "NO"
    egresos["ESTADO_EGRESO"] = "NO PAGADO"
    egresos["FECHA_DE_PAGO"] = ""
    egresos["OBSERVACION"] = ""

    conciliados = 0

    # -----------------------------
    # Conciliación
    # -----------------------------
    for i, e in egresos.iterrows():
        monto = e.get(col_monto_egr)

        if pd.isna(monto):
            egresos.at[i, "OBSERVACION"] = "Egreso sin monto"
            continue

        # buscar candidatos por monto
        candidates = banco[(banco[col_cargo] - float(monto)).abs() <= float(tolerancia)].copy()

        if candidates.empty:
            egresos.at[i, "OBSERVACION"] = "No se encontró cargo en banco"
            continue

        # ⚠️ FILTRAR SOLO FILAS CON FECHA
        candidates = candidates[pd.notna(candidates[col_fecha_banco])]

        if candidates.empty:
            egresos.at[i, "OBSERVACION"] = "Cargo encontrado pero sin fecha en banco"
            continue

        # -----------------------------
        # Scoring
        # -----------------------------
        def score_row(row):
            score = 0.0

            # fecha válida (peso fuerte)
            score += 1000

            # cercanía de fechas
            if col_fecha_egr and pd.notna(e.get(col_fecha_egr)):
                diff_days = abs((e[col_fecha_egr] - row[col_fecha_banco]).days)
                score += max(0, 300 - diff_days)

            # similaridad de texto
            if col_conc_egr and col_desc_banco:
                sim = fuzz.token_set_ratio(
                    str(e.get(col_conc_egr, "")),
                    str(row.get(col_desc_banco, ""))
                )
                score += sim

            return score

        candidates["__SCORE__"] = candidates.apply(score_row, axis=1)

        best = candidates.sort_values("__SCORE__", ascending=False).iloc[0]

        fecha_pago = best[col_fecha_banco]

        if pd.isna(fecha_pago):
            egresos.at[i, "OBSERVACION"] = "Fecha inválida en banco"
            continue

        egresos.at[i, "CONCILIADO_BANCO"] = "SI"
        egresos.at[i, "ESTADO_EGRESO"] = "PAGADO"
        egresos.at[i, "FECHA_DE_PAGO"] = fecha_pago.strftime("%d/%m/%Y")
        egresos.at[i, "OBSERVACION"] = "Conciliado con fecha real de pago"

        conciliados += 1

    resumen = {
        "Egresos totales": int(len(egresos)),
        "Conciliados": int(conciliados),
        "No conciliados": int(len(egresos) - conciliados),
        "Columna cargo banco": col_cargo,
        "Columna fecha banco": col_fecha_banco,
    }

    return egresos, resumen
