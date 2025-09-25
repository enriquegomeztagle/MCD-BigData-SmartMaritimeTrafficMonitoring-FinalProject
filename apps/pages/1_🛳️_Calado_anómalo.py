import streamlit as st
from lib.bq import run_query_df, distinct_values, get_default_dates
from lib.queries import calado_anomalo_query
from lib.ui import chart_bar, DEFAULT_LIMIT

st.header("Calado anómalo (z-score)")

default_start, default_end = get_default_dates()

col1, col2, col3 = st.columns(3)
with col1:
    start = st.date_input("Desde", value=default_start)
with col2:
    end = st.date_input("Hasta", value=default_end)
with col3:
    z_min = st.slider("z mínimo", 0.0, 6.0, 2.0, 0.1)

vtypes = st.multiselect("Tipos de buque", options=distinct_values("VesselTypeName"))
limit = st.number_input("Límite", 50, 5000, DEFAULT_LIMIT, step=50)

sql = calado_anomalo_query(start, end, vtypes, z_min, limit)
df = run_query_df(sql)

st.dataframe(df, width="stretch")
if not df.empty:
    chart_bar(
        df, x="MMSI", y="z", color="VesselTypeName", title="Z-score de calado por MMSI"
    )
