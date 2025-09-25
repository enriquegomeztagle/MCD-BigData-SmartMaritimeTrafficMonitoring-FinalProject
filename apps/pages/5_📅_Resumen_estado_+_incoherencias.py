import streamlit as st
from lib.bq import run_query_df, distinct_values, get_default_dates
from lib.queries import resumen_estado_query
from lib.ui import chart_bar, DEFAULT_LIMIT

st.header("Resumen por estado + incoherencias")

default_start, default_end = get_default_dates()

col1, col2 = st.columns(2)
with col1:
    start = st.date_input("Desde", value=default_start)
with col2:
    end = st.date_input("Hasta", value=default_end)

statuses = st.multiselect(
    "Estados de navegación", options=distinct_values("NavStatusName")
)
sog_thr = st.slider("Umbral SOG para incoherencias", 0.0, 10.0, 2.0, 0.1)
limit = st.number_input("Límite (incoherencias)", 50, 5000, DEFAULT_LIMIT, step=50)

sql = resumen_estado_query(start, end, statuses)
df = run_query_df(sql)

st.subheader("Resumen por estado")
st.dataframe(df, use_container_width=True)
