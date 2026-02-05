from .preprocessing import pick_column


def mover_cancelados_al_final(df):
    """
    Mueve todas las filas cuyo estado contenga 'CANCEL'
    (CANCELADO, CFDI CANCELADO, CANCELADA, etc.) al final del DataFrame.
    """

    col_estado = pick_column(
        df,
        [
            "ESTADO",
            "ESTADO DE PAGO",
            "ESTADO_PAGO",
            "ESTATUS",
            "ESTATUS CFDI",
            "ESTADO CFDI",
            "STATUS",
            "SITUACION",
            "SITUACIÓN"
        ]
    )

    if not col_estado:
        return df

    df = df.copy()

    df["_ORD_CANCELADO_"] = (
        df[col_estado]
        .astype(str)
        .str.upper()
        .str.strip()
        .str.contains("CANCEL", na=False)
    )

    df = (
        df
        .sort_values(by="_ORD_CANCELADO_", ascending=True, kind="stable")
        .drop(columns=["_ORD_CANCELADO_"])
    )

    return df
