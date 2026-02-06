import pandas as pd
from .preprocessing import pick_column, to_money, to_date

#* Funcionalidad que busca los folios iguales en la hoja de complementos del archivo de ingresos

def agrupar_complementos_por_folio(complementos: pd.DataFrame):
    complementos = complementos.copy()
    complementos.columns = complementos.columns.astype(str).str.upper().str.strip()

    col_folio = pick_column(complementos, ["FOLIO"])
    col_importe = pick_column(complementos, ["IMPORTE PAGADO"])
    col_folio_doc = pick_column(complementos, ["FOLIO DOCUMENTO"])
    col_fecha_doc = pick_column(
        complementos,
        ["FECHA EMISION (DOC)", "FECHA EMISION DOC"]
    )
    col_fecha_cp = pick_column(
        complementos,
        ["FECHA EMISION", "FECHA COMPLEMENTO DE PAGO"]
    )

    if not col_folio or not col_importe:
        return complementos

    complementos[col_importe] = to_money(complementos[col_importe]).abs()

    if col_fecha_doc:
        complementos[col_fecha_doc] = to_date(complementos[col_fecha_doc])
    if col_fecha_cp:
        complementos[col_fecha_cp] = to_date(complementos[col_fecha_cp])

    # 🔹 AGRUPAR
    grouped = (
        complementos
        .groupby(col_folio, dropna=False)
        .agg({
            col_importe: "sum",
            col_folio_doc: lambda x: "-".join(
                [str(v) for v in x if pd.notna(v)]
            ),
            col_fecha_doc: "min",
            col_fecha_cp: "min",
        })
        .reset_index()
    )

    return grouped
