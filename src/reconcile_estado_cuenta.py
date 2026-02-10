import re
import pandas as pd
from .preprocessing import pick_column, to_money, to_date
from .config import CARGO_COL_CANDIDATES, FECHA_COL_CANDIDATES, EGRESO_MONTO_CANDIDATES
from .reconcile import conciliar_egresos_vs_banco
from .utils_orden import mover_cancelados_al_final



RESTRICTED_PUE_FORMA = {
    "EFECTIVO",
    "TARJETA CREDITO",
    "TARJETA CRÉDITO",
    "CONDONACION",
    "CONDONACIÓN",
    "NOVACION",
    "NOVACIÓN",
}


def _norm(x):
    return re.sub(r"\s+", " ", str(x or "")).strip().upper()


def _ensure_col(df, col, default=""):
    if col not in df.columns:
        df[col] = default
    return col


def _prepare(df):
    df = df.copy()
    df.columns = df.columns.astype(str).str.upper().str.strip()

    col_monto = pick_column(df, EGRESO_MONTO_CANDIDATES)
    col_folio = pick_column(df, ["FOLIO"])
    col_fecha_em = pick_column(df, ["FECHA EMISION", "FECHA_EMISION"])
    col_metodo = pick_column(df, ["METODO PAGO", "METODO DE PAGO", "METODO_PAGO"])
    col_forma = pick_column(df, ["FORMA PAGO", "FORMA DE PAGO", "FORMA_PAGO"])
    col_estado = pick_column(df, ["ESTADO DE PAGO", "ESTADO_PAGO"])
    col_fecha_pago = pick_column(df, ["FECHA DE PAGO", "FECHA_PAGO"])

    if not col_monto:
        raise ValueError("No se encontró columna de monto")

    if not col_estado:
        col_estado = _ensure_col(df, "ESTADO DE PAGO", "")
    if not col_fecha_pago:
        col_fecha_pago = _ensure_col(df, "FECHA DE PAGO", "")

    df[col_monto] = to_money(df[col_monto]).abs()

    if col_fecha_em:
        df[col_fecha_em] = to_date(df[col_fecha_em])

    df["_USADO_"] = False

    if col_fecha_em:
        df = df.sort_values(col_fecha_em)

    return {
        "df": df,
        "monto": col_monto,
        "folio": col_folio,
        "fecha_em": col_fecha_em,
        "metodo": col_metodo,
        "forma": col_forma,
        "estado": col_estado,
        "fecha_pago": col_fecha_pago,
    }


def conciliar_estado_cuenta_con_movimientos(
    banco: pd.DataFrame,
    ingresos: pd.DataFrame,
    egresos: pd.DataFrame,
    tolerancia: float = 0.01,
):
    banco = banco.copy()
    banco.columns = banco.columns.astype(str).str.upper().str.strip()

    col_cargo = pick_column(banco, CARGO_COL_CANDIDATES)
    col_abono = pick_column(banco, ["ABONO", "ABONOS"])
    col_fecha_banco = pick_column(banco, FECHA_COL_CANDIDATES)

    if not col_fecha_banco:
        raise ValueError("Banco: falta columna FECHA")

    col_folio_fact = pick_column(banco, ["FOLIO FACTURA"])
    col_fecha_fact = pick_column(banco, ["FECHA FACTURA"])

    if not col_folio_fact:
        col_folio_fact = _ensure_col(banco, "FOLIO FACTURA", "")
    if not col_fecha_fact:
        col_fecha_fact = _ensure_col(banco, "FECHA FACTURA", "")

    if col_cargo:
        banco[col_cargo] = to_money(banco[col_cargo]).abs()
    if col_abono:
        banco[col_abono] = to_money(banco[col_abono]).abs()

    banco[col_fecha_banco] = to_date(banco[col_fecha_banco])

    # 🔹 1) Conciliación previa de egresos vs banco (EFECTIVO → PAGADO OTRO)
    egresos_conciliados, _ = conciliar_egresos_vs_banco(
        egresos=egresos,
        banco=banco,
        tolerancia=tolerancia,
    )

    ing = _prepare(ingresos)
    egr = _prepare(egresos_conciliados)

    def match(pack, monto, fecha_pago):
        df = pack["df"]

        cand = df[
            (~df["_USADO_"]) &
            ((df[pack["monto"]] - monto).abs() <= tolerancia)
        ]

        if cand.empty:
            return None

        # Orden por fecha emisión si existe
        if pack["fecha_em"]:
            cand = cand.sort_values(pack["fecha_em"])

        # ✅ Preferir NO-PPD; PPD solo si no hay otra opción
        fallback_ppd = None

        for idx, row in cand.iterrows():
            metodo = _norm(row.get(pack["metodo"]))
            forma = _norm(row.get(pack["forma"]))

            # Guardar primer PPD como fallback, pero NO usarlo todavía
            if "PPD" in metodo:
                if fallback_ppd is None:
                    fallback_ppd = (idx, row)
                continue

            # Restricciones PUE (se marca NO PAGADO y se usa)
            if "PUE" in metodo and any(x in forma for x in RESTRICTED_PUE_FORMA):
                df.at[idx, pack["estado"]] = "NO PAGADO"
                df.at[idx, pack["fecha_pago"]] = ""
                df.at[idx, "_USADO_"] = True
                return row, "NO_PAGADO", pack

            # Normal PAGADO
            df.at[idx, pack["estado"]] = "PAGADO"
            df.at[idx, pack["fecha_pago"]] = (
                fecha_pago.strftime("%d/%m/%Y") if pd.notna(fecha_pago) else ""
            )
            df.at[idx, "_USADO_"] = True
            return row, "PAGADO", pack

        # Si no hubo opción NO-PPD, usar el PPD fallback
        if fallback_ppd is not None:
            idx, row = fallback_ppd
            df.at[idx, pack["estado"]] = "NO PAGADO"
            df.at[idx, pack["fecha_pago"]] = ""
            df.at[idx, "_USADO_"] = True
            return row, "PPD", pack

        return None

    for i, b in banco.iterrows():
        fecha_pago = b[col_fecha_banco]

        res = None

        # EGRESOS → CARGOS
        if col_cargo and pd.notna(b.get(col_cargo)) and b[col_cargo] > 0:
            res = match(egr, b[col_cargo], fecha_pago)

        # INGRESOS → ABONOS
        if not res and col_abono and pd.notna(b.get(col_abono)) and b[col_abono] > 0:
            res = match(ing, b[col_abono], fecha_pago)

        if not res:
            continue

        row, status, pack = res

        # ✅ Banco: FOLIO siempre
        if pack["folio"]:
            banco.at[i, col_folio_fact] = row.get(pack["folio"], "")

        # ✅ Banco: Fecha factura solo si PAGADO (NO para PPD)
        if status == "PAGADO":
            if pack["fecha_em"] and pd.notna(row.get(pack["fecha_em"])):
                banco.at[i, col_fecha_fact] = row[pack["fecha_em"]].strftime("%d/%m/%Y")
        else:
            banco.at[i, col_fecha_fact] = ""  # PPD / NO_PAGADO

    ing["df"].drop(columns=["_USADO_"], inplace=True, errors="ignore")
    egr["df"].drop(columns=["_USADO_"], inplace=True, errors="ignore")

    # 🔹 2) Orden final: CANCELADOS al final
    banco = mover_cancelados_al_final(banco)
    ingresos_out = mover_cancelados_al_final(ing["df"])
    egresos_out = mover_cancelados_al_final(egr["df"])

    return banco, ingresos_out, egresos_out