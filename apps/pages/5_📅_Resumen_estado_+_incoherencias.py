import streamlit as st
from lib.bq import run_query_df, distinct_values
from lib.ui import chart_bar, DEFAULT_LIMIT

st.header("Resumen por estado + incoherencias")

col1, col2 = st.columns(2)
with col1:
    start = st.date_input("Desde", value=None)
with col2:
    end = st.date_input("Hasta", value=None)

statuses = st.multiselect("Estados de navegación", options=distinct_values("NavStatusName"))
sog_thr = st.slider("Umbral SOG para incoherencias", 0.0, 10.0, 2.0, 0.1)
limit = st.number_input("Límite (incoherencias)", 50, 5000, DEFAULT_LIMIT, step=50)

params_common = {
    "start_date": str(start) if start else None,
    "end_date": str(end) if end else None,
}

# Resumen
sql_sum = open("sql/resumen_estado.sql", "r", encoding="utf-8").read()
df_sum = run_query_df(sql_sum, {**params_common, "nav_status": statuses})

st.subheader("Resumen por estado")
st.dataframe(df_sum, use_container_width=True)