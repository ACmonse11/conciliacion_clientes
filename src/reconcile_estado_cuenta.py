import re
import pandas as pd

from .preprocessing import pick_column, to_money, to_date
from .config import CARGO_COL_CANDIDATES, FECHA_COL_CANDIDATES, EGRESO_MONTO_CANDIDATES
from .reconcile import conciliar_egresos_vs_banco
from .utils_orden import mover_cancelados_al_final
from .reconcile_publico_general import conciliar_publico_en_general_subset


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

    # Estado/fecha de pago (salida)
    col_estado_pago = pick_column(df, ["ESTADO DE PAGO", "ESTADO_PAGO"])
    col_fecha_pago = pick_column(df, ["FECHA DE PAGO", "FECHA_PAGO"])

    # Estado fiscal real (entrada) -> aquí es donde aparece CANCELADO
    col_estado_cfdi = pick_column(df, ["ESTADO", "ESTATUS", "ESTADO CFDI", "ESTATUS CFDI", "STATUS", "SITUACION", "SITUACIÓN"])

    if not col_monto:
        raise ValueError("No se encontró columna de monto")

    if not col_estado_pago:
        col_estado_pago = _ensure_col(df, "ESTADO DE PAGO", "")
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
        "estado": col_estado_pago,      # columna de salida: ESTADO DE PAGO
        "fecha_pago": col_fecha_pago,   # columna de salida: FECHA DE PAGO
        "estado_cfdi": col_estado_cfdi  # columna real del CFDI: CANCELADO/VIGENTE/etc
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

    # 🔹 1) Conciliación previa de egresos vs banco
    egresos_conciliados, _ = conciliar_egresos_vs_banco(
        egresos=egresos,
        banco=banco,
        tolerancia=tolerancia,
    )

    ing = _prepare(ingresos)
    egr = _prepare(egresos_conciliados)

    # =========================================================
    # ✅ MARCAR CANCELADOS DESDE EL INICIO (CLAVE)
    # Esto evita que terminen como "PENDIENTE DE IDENTIFICAR"
    # =========================================================
    for pack in (ing, egr):
        dfp = pack["df"]
        col_estado_cfdi = pack.get("estado_cfdi")
        if col_estado_cfdi:
            mask_cancelado = (
                dfp[col_estado_cfdi]
                .astype(str)
                .str.upper()
                .str.contains("CANCEL", na=False)
            )
            # Estado de pago = CANCELADO
            dfp.loc[mask_cancelado, pack["estado"]] = "CANCELADO"
            dfp.loc[mask_cancelado, pack["fecha_pago"]] = ""
            # marcar como usado para que no se pisen al final
            dfp.loc[mask_cancelado, "_USADO_"] = True

    # =========================================================
    # UUID relacionados (tu lógica original)
    # =========================================================
    def _to_money_scalar(x) -> float:
        return float(pd.to_numeric(str(x).replace(",", ""), errors="coerce"))

    df_egr = egr["df"]

    col_tipo_egr = pick_column(df_egr, ["TIPO"])
    col_uuid_egr = pick_column(df_egr, ["UUID"])
    col_uuid_rel = pick_column(df_egr, ["UUIDS RELACIONADOS"])
    col_total_egr = pick_column(df_egr, ["TOTAL"])
    col_folio_egr = pick_column(df_egr, ["FOLIO"])

    if all([col_tipo_egr, col_uuid_egr, col_uuid_rel, col_total_egr, col_folio_egr, col_cargo]):

        uuid_index = (
            df_egr[[col_uuid_egr]]
            .astype(str)
            .apply(lambda s: s.str.strip())
        )
        df_egr["_UUID_NORM_"] = uuid_index[col_uuid_egr]
        lookup = df_egr.set_index("_UUID_NORM_", drop=False)

        for _, row in df_egr.iterrows():
            tipo = str(row.get(col_tipo_egr, "")).upper().strip()
            if "EGRESO" not in tipo:
                continue

            uuid_rel_val = str(row.get(col_uuid_rel, "")).strip()
            if not uuid_rel_val:
                continue

            if uuid_rel_val not in lookup.index:
                continue

            row_rel = lookup.loc[uuid_rel_val]
            if isinstance(row_rel, pd.DataFrame):
                row_rel = row_rel.iloc[0]

            total_rel = _to_money_scalar(row_rel.get(col_total_egr))
            total_egr = _to_money_scalar(row.get(col_total_egr))

            if pd.isna(total_rel) or pd.isna(total_egr):
                continue

            monto_final = round(abs(total_rel) - abs(total_egr), 2)

            cand_banco = banco[
                (banco[col_cargo].notna()) &
                ((banco[col_cargo] - monto_final).abs() <= tolerancia)
            ]
            if cand_banco.empty:
                continue

            mov = cand_banco.iloc[0]

            def _clean_folio(x):
                if pd.isna(x):
                    return ""
                try:
                    return str(int(float(x)))
                except Exception:
                    return str(x).strip()

            folio_rel = _clean_folio(row_rel.get(col_folio_egr))
            folio_egr = _clean_folio(row.get(col_folio_egr))

            if folio_rel and folio_egr:
                banco.at[mov.name, col_folio_fact] = f"{folio_rel}-{folio_egr}"

        df_egr.drop(columns=["_UUID_NORM_"], inplace=True, errors="ignore")

    # =========================================================
    # MATCH
    # =========================================================
    def match(pack, monto, fecha_pago):
        df = pack["df"]

        cand = df[
            (~df["_USADO_"]) &
            ((df[pack["monto"]] - monto).abs() <= tolerancia)
        ]

        if cand.empty:
            return None

        if pack["fecha_em"]:
            cand = cand.sort_values(pack["fecha_em"])

        fallback_ppd = None

        for idx, row in cand.iterrows():
            metodo = _norm(row.get(pack["metodo"]))
            forma = _norm(row.get(pack["forma"]))

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

        if fallback_ppd is not None:
            idx, row = fallback_ppd
            df.at[idx, pack["estado"]] = "NO PAGADO"
            df.at[idx, pack["fecha_pago"]] = ""
            df.at[idx, "_USADO_"] = True
            return row, "PPD", pack

        return None

    # =========================================================
    # RECORRER BANCO Y CONCILIAR
    # =========================================================
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

        # Banco: FOLIO siempre
        if pack["folio"]:
            banco.at[i, col_folio_fact] = row.get(pack["folio"], "")

        # Banco: Fecha factura solo si PAGADO
        if status == "PAGADO":
            if pack["fecha_em"] and pd.notna(row.get(pack["fecha_em"])):
                banco.at[i, col_fecha_fact] = row[pack["fecha_em"]].strftime("%d/%m/%Y")
        else:
            banco.at[i, col_fecha_fact] = ""

            ing["df"], banco = conciliar_publico_en_general_subset(
                ingresos=ing["df"],
                banco=banco,
                tolerancia=tolerancia,
            )

    # =========================================================
    # Marcar ingresos no conciliados (sin pisar CANCELADOS)
    # =========================================================
    df_ing = ing["df"]
    mask_no_conciliado = ~df_ing["_USADO_"]
    df_ing.loc[mask_no_conciliado, ing["estado"]] = "PENDIENTE DE IDENTIFICAR"

    # =========================================================
    # Limpieza y orden final
    # =========================================================
    ing["df"].drop(columns=["_USADO_"], inplace=True, errors="ignore")
    egr["df"].drop(columns=["_USADO_"], inplace=True, errors="ignore")

    banco = mover_cancelados_al_final(banco)
    ingresos_out = mover_cancelados_al_final(ing["df"])
    egresos_out = mover_cancelados_al_final(egr["df"])

    for df in [ingresos_out, egresos_out]:
        if "FECHA DE PAGO" in df.columns:
            df["FECHA DE PAGO"] = (
                pd.to_datetime(df["FECHA DE PAGO"], errors="coerce", dayfirst=True)
                .dt.strftime("%d/%m/%Y")
            )

    return banco, ingresos_out, egresos_out
