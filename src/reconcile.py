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
from .utils_orden import mover_cancelados_al_final


def conciliar_egresos_vs_banco(
    egresos: pd.DataFrame,
    banco: pd.DataFrame,
    tolerancia: float = 1.0
):
    col_cargo = pick_column(banco, CARGO_COL_CANDIDATES)
    col_fecha_banco = pick_column(banco, FECHA_COL_CANDIDATES)
    col_desc_banco = pick_column(banco, DESCRIP_COL_CANDIDATES)

    col_monto_egr = pick_column(egresos, EGRESO_MONTO_CANDIDATES)
    col_fecha_egr = pick_column(egresos, EGRESO_FECHA_CANDIDATES)
    col_conc_egr = pick_column(egresos, EGRESO_CONCEPTO_CANDIDATES)
    col_forma_pago = pick_column(
        egresos,
        ["FORMA PAGO", "FORMA DE PAGO", "METODO DE PAGO"]
    )

    banco = banco.copy()
    banco[col_cargo] = to_money(banco[col_cargo]).abs()
    banco[col_fecha_banco] = to_date(banco[col_fecha_banco])

    egresos = egresos.copy()
    egresos[col_monto_egr] = to_money(egresos[col_monto_egr]).abs()

    if col_fecha_egr:
        egresos["_FECHA_EMISION_DT"] = to_date(egresos[col_fecha_egr])
    else:
        egresos["_FECHA_EMISION_DT"] = pd.NaT

    # Columnas internas
    egresos["CONCILIADO_BANCO"] = "NO"
    egresos["ESTADO_EGRESO"] = "NO LOCALIZADO"
    egresos["FECHA_DE_PAGO"] = ""
    egresos["OBSERVACION"] = ""

    for i, e in egresos.iterrows():
        monto = e.get(col_monto_egr)
        if pd.isna(monto):
            continue

        # EFECTIVO → PAGADO OTRO
        if col_forma_pago:
            forma = str(e.get(col_forma_pago, "")).upper().strip()
            if "EFECTIVO" in forma or forma == "01":
                egresos.at[i, "CONCILIADO_BANCO"] = "SI"
                egresos.at[i, "ESTADO_EGRESO"] = "PAGADO OTRO"
                egresos.at[i, "FECHA_DE_PAGO"] = ""
                egresos.at[i, "OBSERVACION"] = "Pago en efectivo (no bancario)"
                continue

        candidatos = banco[
            (banco[col_cargo] - monto).abs() <= tolerancia
        ]

        if candidatos.empty:
            continue

        candidatos = candidatos[pd.notna(candidatos[col_fecha_banco])]
        if candidatos.empty:
            continue

        def score(row):
            s = 1000
            if col_fecha_egr and pd.notna(e["_FECHA_EMISION_DT"]):
                s += max(
                    0,
                    300 - abs(
                        (e["_FECHA_EMISION_DT"] - row[col_fecha_banco]).days
                    )
                )
            if col_conc_egr and col_desc_banco:
                s += fuzz.token_set_ratio(
                    str(e.get(col_conc_egr, "")),
                    str(row.get(col_desc_banco, ""))
                )
            return s

        candidatos = candidatos.copy()
        candidatos["__SCORE__"] = candidatos.apply(score, axis=1)
        best = candidatos.sort_values("__SCORE__", ascending=False).iloc[0]

        egresos.at[i, "CONCILIADO_BANCO"] = "SI"
        egresos.at[i, "ESTADO_EGRESO"] = "PAGADO"
        egresos.at[i, "FECHA_DE_PAGO"] = best[col_fecha_banco].strftime("%d/%m/%Y")
        egresos.at[i, "OBSERVACION"] = "Conciliado con estado de cuenta"

    # 🔹 SINCRONIZAR columnas originales
    col_estado_original = pick_column(egresos, ["ESTADO DE PAGO", "ESTADO_PAGO"])
    col_fecha_original = pick_column(egresos, ["FECHA DE PAGO", "FECHA_PAGO"])
    col_obs_original = pick_column(egresos, ["OBSERVACIONES", "OBSERVACION"])

    if col_estado_original:
        egresos[col_estado_original] = egresos["ESTADO_EGRESO"]

    if col_fecha_original:
        egresos[col_fecha_original] = egresos["FECHA_DE_PAGO"]

    if col_obs_original:
        egresos[col_obs_original] = egresos["OBSERVACION"]

    egresos.drop(columns=["_FECHA_EMISION_DT"], inplace=True, errors="ignore")

    # 🔹 Orden final
    egresos = mover_cancelados_al_final(egresos)

    # 🔹 Eliminar columnas internas duplicadas (solo visual)
    egresos.drop(
        columns=["ESTADO_EGRESO", "FECHA_DE_PAGO", "OBSERVACION"],
        errors="ignore",
        inplace=True
    )

    resumen = {
        "Egresos totales": len(egresos),
        "Pagados": (egresos[col_estado_original] == "PAGADO").sum() if col_estado_original else 0,
        "Pagados otro": (egresos[col_estado_original] == "PAGADO OTRO").sum() if col_estado_original else 0,
        "No localizados": (egresos[col_estado_original] == "NO LOCALIZADO").sum() if col_estado_original else 0,
    }

    return egresos, resumen
