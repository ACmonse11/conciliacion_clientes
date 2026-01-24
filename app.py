import streamlit as st
import pandas as pd

from src.loaders import read_excel_any
from src.reconcile import conciliar_egresos_vs_banco
from src.reconcile_ingresos import conciliar_ingresos_vs_banco
from src.reconcile_estado_cuenta import conciliar_estado_cuenta_con_egresos
from src.export import to_excel_bytes

# =============================
# CONFIGURACIÓN STREAMLIT
# =============================
st.set_page_config(
    page_title="Conciliación Bancaria",
    layout="wide"
)

st.title("Sistema de Conciliación Bancaria")

# =============================
# SELECCIÓN DE TIPO
# =============================
tipo = st.radio(
    "¿Qué deseas conciliar?",
    ["EGRESOS", "INGRESOS"],
    horizontal=True
)

col1, col2 = st.columns(2)

with col1:
    archivo_principal = st.file_uploader(
        f"1) Sube Excel de {tipo}",
        type=["xlsx"]
    )

with col2:
    banco_file = st.file_uploader(
        "2) Sube Excel de ESTADO DE CUENTA (BANCO)",
        type=["xlsx"]
    )

tolerancia = st.number_input(
    "Tolerancia de monto",
    min_value=0.0,
    value=1.0,
    step=0.5
)

# =============================
# BOTÓN CONCILIAR
# =============================
if st.button("Conciliar"):
    if not archivo_principal or not banco_file:
        st.error("Debes subir ambos archivos.")
        st.stop()

    datos = read_excel_any(archivo_principal)
    banco = read_excel_any(banco_file)

    # =============================
    # LIMPIAR COLUMNAS UNNAMED
    # =============================
    datos = datos.loc[:, ~datos.columns.str.contains("^UNNAMED", case=False)]
    banco = banco.loc[:, ~banco.columns.str.contains("^UNNAMED", case=False)]

    with st.spinner("Conciliando..."):
        if tipo == "EGRESOS":
            resultado, resumen = conciliar_egresos_vs_banco(
                datos,
                banco,
                tolerancia=tolerancia
            )
            nombre_archivo = "EGRESOS_CONCILIADOS.xlsx"
            sheet_name = "EGRESOS_CONCILIADOS"

        else:  # INGRESOS
            resultado, resumen = conciliar_ingresos_vs_banco(
                datos,
                banco,
                tolerancia=tolerancia
            )
            nombre_archivo = "INGRESOS_CONCILIADOS.xlsx"
            sheet_name = "INGRESOS_CONCILIADOS"

    st.success("Conciliación terminada ✅")

    # =============================
    # RESUMEN
    # =============================
    st.subheader("Resumen")
    st.json(resumen)

    # =============================
    # VISTA PREVIA
    # =============================
    st.subheader("Vista previa (primeras 50 filas)")
    st.dataframe(
        resultado.head(50),
        use_container_width=True
    )

    # =============================
    # DESCARGA ARCHIVO CONCILIADO
    # =============================
    excel_bytes = to_excel_bytes(
        resultado,
        sheet_name=sheet_name
    )

    st.download_button(
        f"Descargar {nombre_archivo}",
        data=excel_bytes,
        file_name=nombre_archivo,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    st.divider()
    st.subheader("Estado de cuenta conciliado")

    banco_conciliado = conciliar_estado_cuenta_con_egresos(
        banco=banco,
        egresos=datos
    )

        # =============================
        # VISTA PREVIA
        # =============================
    st.subheader("Vista previa Estado de Cuenta (primeras 50 filas)")
    st.dataframe(
        banco_conciliado.head(50),
        use_container_width=True
    )

        # =============================
        # DESCARGA
        # =============================
    banco_excel = to_excel_bytes(
           banco_conciliado,
        sheet_name="ESTADO_CUENTA_CONCILIADO"
    )

    st.download_button(
        "Descargar ESTADO_CUENTA_CONCILIADO.xlsx",
        data=banco_excel,
        file_name="ESTADO_CUENTA_CONCILIADO.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
