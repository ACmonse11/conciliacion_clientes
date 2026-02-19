import pandas as pd
from .preprocessing import pick_column, to_money


def aplicar_notas_credito_por_uuid(df: pd.DataFrame):

    df = df.copy()
    df.columns = df.columns.astype(str).str.upper().str.strip()

    col_uuid = pick_column(df, ["UUID"])
    col_uuid_rel = pick_column(df, ["UUIDS RELACIONADOS"])
    col_tipo = pick_column(df, ["TIPO"])
    col_tipo_rel = pick_column(df, ["TIPO RELACION", "TIPO DE RELACION"])
    col_total = pick_column(df, ["TOTAL"])

    if not all([col_uuid, col_uuid_rel, col_tipo, col_tipo_rel, col_total]):
        return df

    df[col_total] = to_money(df[col_total])
    df["_UUID_NORM_"] = df[col_uuid].astype(str).str.strip()

    lookup = df.set_index("_UUID_NORM_", drop=False)

    df["MONTO_AJUSTADO"] = df[col_total]

    for idx, row in df.iterrows():

        tipo = str(row[col_tipo]).upper().strip()
        tipo_rel = str(row[col_tipo_rel]).upper().strip()

        # 🔥 Solo EGRESO con tipo relación 01
        if "EGRESO" not in tipo:
            continue

        if "01" not in tipo_rel:
            continue

        uuid_rel = str(row[col_uuid_rel]).strip()
        if not uuid_rel:
            continue

        if uuid_rel not in lookup.index:
            continue

        row_rel = lookup.loc[uuid_rel]

        if isinstance(row_rel, pd.DataFrame):
            row_rel = row_rel.iloc[0]

        total_original = abs(row_rel[col_total])
        total_nota = abs(row[col_total])

        monto_final = round(total_original - total_nota, 2)

        # Actualizar monto ajustado en la factura original
        df.loc[df[col_uuid] == uuid_rel, "MONTO_AJUSTADO"] = monto_final

    df.drop(columns=["_UUID_NORM_"], inplace=True, errors="ignore")

    return df
