import pandas as pd

from .preprocessing import pick_column, to_money, to_date
from .config import (
    EGRESO_MONTO_CANDIDATES,
    EGRESO_FECHA_CANDIDATES,
    FECHA_COL_CANDIDATES,
)


def conciliar_publico_en_general_por_suma(
    ingresos: pd.DataFrame,
    banco: pd.DataFrame,
    tolerancia: float = 0.01,
):
    ingresos = ingresos.copy()
    banco = banco.copy()

    # =============================
    # COLUMNAS INGRESOS
    # =============================
    col_razon = pick_column(
        ingresos,
        ["RAZON RECEPTOR", "RAZON_RECEPTOR", "RAZON"]
    )
    col_monto = pick_column(ingresos, EGRESO_MONTO_CANDIDATES)
    col_estado = pick_column(ingresos, ["ESTADO DE PAGO", "ESTADO_PAGO"])
    col_fecha_pago = pick_column(ingresos, ["FECHA DE PAGO", "FECHA_PAGO"])

    # =============================
    # COLUMNAS BANCO
    # =============================
    col_abono = pick_column(
        banco,
        ["ABONO", "ABONOS", "CREDITO", "CRÉDITO", "DEPOSITO", "DEPÓSITO"]
    )
    col_fecha_banco = pick_column(banco, FECHA_COL_CANDIDATES)

    if not col_razon or not col_monto or not col_abono:
        return ingresos, banco

    # =============================
    # NORMALIZAR
    # =============================
    ingresos[col_monto] = to_money(ingresos[col_monto]).abs()
    banco[col_abono] = to_money(banco[col_abono]).abs()

    if col_fecha_banco:
        banco[col_fecha_banco] = to_date(banco[col_fecha_banco])

    # =============================
    # CONCILIACIÓN POR SUMA
    # =============================
    for i, ing in ingresos.iterrows():

        # 🔒 No reprocesar pagados
        if col_estado and str(ing.get(col_estado, "")).upper() == "PAGADO":
            continue

        razon = str(ing.get(col_razon, "")).upper().strip()
        if "PUBLICO" not in razon:
            continue

        monto_objetivo = ing[col_monto]
        if pd.isna(monto_objetivo) or monto_objetivo <= 0:
            continue

        # 🔑 SOLO ABONOS POSITIVOS, ORDENADOS
        disponibles = banco[banco[col_abono] > 0].sort_values(col_abono)

        suma = 0
        usados = []

        for idx, row in disponibles.iterrows():
            suma += row[col_abono]
            usados.append(idx)

            if abs(suma - monto_objetivo) <= tolerancia:
                # ✅ INGRESO PAGADO
                if col_estado:
                    ingresos.at[i, col_estado] = "PAGADO"

                if col_fecha_pago and col_fecha_banco:
                    fecha = row[col_fecha_banco]
                    if pd.notna(fecha):
                        ingresos.at[i, col_fecha_pago] = fecha.strftime("%d/%m/%Y")

                ingresos.at[i, "OBSERVACION"] = (
                    f"PUBLICO EN GENERAL - Conciliado por suma ({len(usados)} abonos)"
                )
                break

        # si no encontró suma, sigue con el siguiente ingreso

    return ingresos, banco
