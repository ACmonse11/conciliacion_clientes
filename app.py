import streamlit as st
import pandas as pd
from src.loaders import read_excel_any
from src.reconcile import conciliar_egresos_vs_banco
from src.export import to_excel_bytes

st.set_page_config(page_title="Conciliación", layout="wide")
st.title("Sistema de Conciliación Bancaria (Egresos vs Banco)")

col1, col2 = st.columns(2)

with col1:
    egresos_file = st.file_uploader("1) Sube Excel de EGRESOS", type=["xlsx"])

with col2:
    banco_file = st.file_uploader("2) Sube Excel de ESTADO DE CUENTA (BANCO)", type=["xlsx"])

tolerancia = st.number_input("Tolerancia de monto (pesos)", min_value=0.0, value=1.0, step=0.5)

if st.button("Conciliar"):
    if not egresos_file or not banco_file:
        st.error("Debes subir ambos archivos.")
        st.stop()

    egresos = read_excel_any(egresos_file)
    banco = read_excel_any(banco_file)

    with st.spinner("Conciliando..."):
        resultado, resumen = conciliar_egresos_vs_banco(egresos, banco, tolerancia=tolerancia)

    st.success("Conciliación terminada ✅")

    st.subheader("Resumen")
    st.json(resumen)

    st.subheader("Vista previa (primeras 50 filas)")
    st.dataframe(resultado.head(50), use_container_width=True)

    excel_bytes = to_excel_bytes(resultado, sheet_name="EGRESOS_CONCILIADOS")
    st.download_button(
        "Descargar EGRESOS_CONCILIADOS.xlsx",
        data=excel_bytes,
        file_name="EGRESOS_CONCILIADOS.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
