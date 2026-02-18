import pandas as pd
import unicodedata

def _norm_no_accents(s: str) -> str:
    s = str(s or "").strip().upper()
    return unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("ASCII")


def conciliar_publico_en_general_subset(
    ingresos: pd.DataFrame,
    banco: pd.DataFrame,
    tolerancia: float = 0.01,
):
    ingresos = ingresos.copy()
    banco = banco.copy()

    ingresos.columns = ingresos.columns.astype(str).str.upper().str.strip()
    banco.columns = banco.columns.astype(str).str.upper().str.strip()

    def pick(df, candidates):
        cols = {c.upper().strip(): c for c in df.columns}
        for cand in candidates:
            if cand.upper().strip() in cols:
                return cols[cand.upper().strip()]
        return None

    col_razon = pick(ingresos, ["RAZON RECEPTOR", "RAZON_RECEPTOR", "RAZON"])
    col_total = pick(ingresos, ["TOTAL", "MONTO", "IMPORTE"])
    col_estado = pick(ingresos, ["ESTADO DE PAGO", "ESTADO_PAGO"])
    col_fecha_pago = pick(ingresos, ["FECHA DE PAGO", "FECHA_PAGO"])
    col_folio_ing = pick(ingresos, ["FOLIO"])
    col_fecha_em_ing = pick(ingresos, ["FECHA EMISION", "FECHA_EMISION"])

    col_abono = pick(banco, ["ABONOS", "ABONO"])
    col_fecha_banco = pick(banco, ["FECHA"])
    col_folio_fact = pick(banco, ["FOLIO FACTURA"])
    col_fecha_fact = pick(banco, ["FECHA FACTURA"])

    if not all([col_razon, col_total, col_abono, col_fecha_banco, col_folio_fact]):
        return ingresos, banco

    if not col_estado:
        col_estado = "ESTADO DE PAGO"
        ingresos[col_estado] = ""

    if not col_fecha_pago:
        col_fecha_pago = "FECHA DE PAGO"
        ingresos[col_fecha_pago] = ""

    ingresos[col_total] = pd.to_numeric(ingresos[col_total], errors="coerce").fillna(0).abs().round(2)
    banco[col_abono] = pd.to_numeric(banco[col_abono], errors="coerce").fillna(0).abs().round(2)
    banco[col_fecha_banco] = pd.to_datetime(banco[col_fecha_banco], errors="coerce")

    # =============================
    # CONCILIACIÓN POR SUMA
    # =============================
    for i, ing in ingresos.iterrows():

        estado = _norm_no_accents(ing.get(col_estado, ""))
        if estado == "PAGADO":
            continue

        razon = _norm_no_accents(ing.get(col_razon, ""))
        if "PUBLICO" not in razon:
            continue

        target = float(ing.get(col_total, 0))
        if target <= 0:
            continue

        folio_vacio = banco[col_folio_fact].isna() | (banco[col_folio_fact].astype(str).str.strip() == "")
        pool = banco[(banco[col_abono] > 0) & folio_vacio].copy()

        if pool.empty:
            continue

        vals = (pool[col_abono] * 100).astype(int).tolist()
        idxs = pool.index.tolist()
        target_cents = int(round(target * 100))

        dp = {0: None}
        found_sum = None

        for pos, v in enumerate(vals):
            for s in list(dp.keys())[::-1]:
                ns = s + v
                if ns > target_cents:
                    continue
                if ns not in dp:
                    dp[ns] = (s, pos)
                if ns == target_cents:
                    found_sum = ns
                    break
            if found_sum is not None:
                break

        if found_sum is None:
            continue

        # reconstruir combinación
        usados_pos = []
        cur = found_sum
        while cur != 0:
            prev, pos = dp[cur]
            usados_pos.append(pos)
            cur = prev

        usados_idx = [idxs[p] for p in usados_pos]

        # ✅ Marcar ingreso pagado
        ingresos.at[i, col_estado] = "PAGADO"

        fechas = (
            banco.loc[usados_idx, col_fecha_banco]
            .dropna()
            .dt.strftime("%d/%m/%Y")
            .tolist()
        )
        ingresos.at[i, col_fecha_pago] = " - ".join(fechas)

        # ✅ Poner folio real y fecha en banco
        folio_ingreso = ing.get(col_folio_ing, "") if col_folio_ing else ""
        fecha_emision = ing.get(col_fecha_em_ing) if col_fecha_em_ing else None

        for idx in usados_idx:
            banco.at[idx, col_folio_fact] = folio_ingreso

            if col_fecha_fact and fecha_emision is not None and pd.notna(fecha_emision):
                banco.at[idx, col_fecha_fact] = fecha_emision.strftime("%d/%m/%Y")

        if "OBSERVACIONES" in ingresos.columns:
            ingresos.at[i, "OBSERVACIONES"] = f"Conciliado PUBLICO EN GENERAL ({len(usados_idx)} abonos)"

    return ingresos, banco
