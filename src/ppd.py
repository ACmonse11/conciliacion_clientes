from .complementos import agrupar_complementos_por_folio
from .preprocessing import pick_column
import pandas as pd


def procesar_ppd(acumulados: pd.DataFrame, complementos: pd.DataFrame):

    acumulados = acumulados.copy()
    acumulados.columns = acumulados.columns.astype(str).str.upper().str.strip()

    col_folio = pick_column(acumulados, ["FOLIO"])
    col_metodo = pick_column(acumulados, ["METODO PAGO", "METODO DE PAGO"])

    if not col_folio or not col_metodo:
        return pd.DataFrame()

    ppd = acumulados[
        acumulados[col_metodo].astype(str).str.upper().str.strip() == "PPD"
    ]

    if ppd.empty:
        return pd.DataFrame()

    complementos_grouped = agrupar_complementos_por_folio(complementos)

    resultado = ppd.merge(
        complementos_grouped,
        on=col_folio,
        how="left"
    )

    return resultado
