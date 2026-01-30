import streamlit as st
import pandas as pd

from src.loaders import read_excel_any
from src.export import to_excel_bytes, to_excel_multiple_sheets

from src.reconcile_estado_cuenta import conciliar_estado_cuenta_con_movimientos
from src.reconcile_ppd_complementos import conciliar_ppd_desde_complementos
from src.reconcile_ingresos_abonos import conciliar_ingresos_con_abonos

# =====================================
# CONFIGURACIÓN STREAMLIT
# =====================================
st.set_page_config(
    page_title="Conciliación Bancaria",
    layout="wide"
)

st.title("Sistema de Conciliación Bancaria")
st.caption("Conserva hojas (ACUMULADO/COMPLEMENTOS/NÓMINA, etc.) y muestra vistas previas")

# =====================================
# CARGA DE ARCHIVOS
# =====================================
col1, col2, col3 = st.columns(3)

with col1:
    egresos_file = st.file_uploader("1️⃣ Egresos (multi-hoja)", type=["xlsx"])

with col2:
    ingresos_file = st.file_uploader("2️⃣ Ingresos (multi-hoja)", type=["xlsx"])

with col3:
    banco_file = st.file_uploader("3️⃣ Estado de Cuenta (Banco)", type=["xlsx"])

tolerancia = st.number_input(
    "Tolerancia de monto",
    min_value=0.0,
    value=0.01,
    step=0.01
)

# =====================================
# BOTÓN
# =====================================
if st.button("Conciliar"):
    if not egresos_file or not ingresos_file or not banco_file:
        st.error("Debes subir los tres archivos.")
        st.stop()

    # =====================================
    # ===== INGRESOS (TODAS LAS HOJAS)
    # =====================================
    xls_ing = pd.ExcelFile(ingresos_file)
    ingresos_sheets = {}

    for sheet in xls_ing.sheet_names:
        df = xls_ing.parse(sheet)
        df.columns = df.columns.astype(str).str.upper().str.strip()
        df = df.loc[:, ~df.columns.str.contains("^UNNAMED", case=False)]
        ingresos_sheets[sheet] = df

    if "ACUMULADO" not in ingresos_sheets:
        st.error("El archivo de INGRESOS debe tener una hoja llamada 'ACUMULADO'")
        st.stop()

    ingresos_acumulado = ingresos_sheets["ACUMULADO"]
    ingresos_complementos = ingresos_sheets.get("COMPLEMENTOS")  # puede existir o no

    # =====================================
    # ===== EGRESOS (TODAS LAS HOJAS)
    # =====================================
    xls_egr = pd.ExcelFile(egresos_file)
    egresos_sheets = {}

    for sheet in xls_egr.sheet_names:
        df = xls_egr.parse(sheet)
        df.columns = df.columns.astype(str).str.upper().str.strip()
        df = df.loc[:, ~df.columns.str.contains("^UNNAMED", case=False)]
        egresos_sheets[sheet] = df

    if "ACUMULADO" not in egresos_sheets:
        st.error("El archivo de EGRESOS debe tener una hoja llamada 'ACUMULADO'")
        st.stop()

    egresos_acumulado = egresos_sheets["ACUMULADO"]

    # =====================================
    # ===== BANCO
    # =====================================
    banco = read_excel_any(banco_file)
    banco = banco.loc[:, ~banco.columns.str.contains("^UNNAMED", case=False)]
    banco.columns = banco.columns.astype(str).str.upper().str.strip()


    # =====================================
    # ===== CONCILIACIONES
    # =====================================
    with st.spinner("Conciliando información..."):
        # 1) Conciliación principal: Estado de cuenta ↔ (Ingresos ACUMULADO + Egresos ACUMULADO)
        banco_out, ingresos_out, egresos_out = conciliar_estado_cuenta_con_movimientos(
            banco=banco,
            ingresos=ingresos_acumulado,
            egresos=egresos_acumulado,
            tolerancia=tolerancia
        )

        # 2) PPD desde COMPLEMENTOS (si existe hoja)
        if ingresos_complementos is not None and not ingresos_complementos.empty:
            banco_out, ingresos_out = conciliar_ppd_desde_complementos(
                ingresos_acumulado=ingresos_out,
                complementos=ingresos_complementos,
                banco=banco_out,
                tolerancia=tolerancia
            )

        # 3) Ingresos directos vs ABONOS del banco: marcar PAGADO + FECHA DE PAGO
        ingresos_out = conciliar_ingresos_con_abonos(
            ingresos=ingresos_out,
            banco=banco_out,
            tolerancia=tolerancia
        )

    # =====================================
    # ===== REEMPLAZAR HOJAS ACTUALIZADAS (SIN BORRAR LAS DEMÁS)
    # =====================================
    ingresos_sheets["ACUMULADO"] = ingresos_out
    egresos_sheets["ACUMULADO"] = egresos_out

    # =====================================
    # ===== VISTAS PREVIAS
    # =====================================
    st.success("Conciliación terminada ✅")

    st.divider()
    st.subheader("Vista previa - Estado de Cuenta conciliado (primeras 50 filas)")
    st.dataframe(banco_out.head(50), use_container_width=True)

    st.subheader("Vista previa - Ingresos (ACUMULADO actualizado) (primeras 50 filas)")
    st.dataframe(ingresos_out.head(50), use_container_width=True)

    if ingresos_complementos is not None:
        st.subheader("Vista previa - Ingresos (COMPLEMENTOS original) (primeras 50 filas)")
        st.dataframe(ingresos_complementos.head(50), use_container_width=True)

    st.subheader("Vista previa - Egresos (ACUMULADO actualizado) (primeras 50 filas)")
    st.dataframe(egresos_out.head(50), use_container_width=True)

    # =====================================
    # ===== DESCARGAS
    # =====================================
    st.divider()
    st.subheader("Descargar archivos")

    banco_excel = to_excel_bytes(
        banco_out,
        sheet_name="ESTADO_CUENTA_CONCILIADO"
    )

    ingresos_excel = to_excel_multiple_sheets(ingresos_sheets)
    egresos_excel = to_excel_multiple_sheets(egresos_sheets)

    c1, c2, c3 = st.columns(3)

    with c1:
        st.download_button(
            "⬇️ Estado de Cuenta",
            data=banco_excel,
            file_name="ESTADO_CUENTA_CONCILIADO.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    with c2:
        st.download_button(
            "⬇️ Ingresos (todas las hojas)",
            data=ingresos_excel,
            file_name="INGRESOS_ACTUALIZADOS.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    with c3:
        st.download_button(
            "⬇️ Egresos (todas las hojas)",
            data=egresos_excel,
            file_name="EGRESOS_ACTUALIZADOS.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
