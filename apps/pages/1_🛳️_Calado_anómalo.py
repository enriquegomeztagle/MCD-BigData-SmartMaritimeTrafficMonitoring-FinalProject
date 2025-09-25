import streamlit as st
from datetime import date
from lib.bq import run_query_df, distinct_values
from lib.ui import chart_bar, DEFAULT_LIMIT

st.header("Calado anómalo (z-score)")

col1, col2, col3 = st.columns(3)
with col1:
    start = st.date_input("Desde", value=None)
with col2:
    end = st.date_input("Hasta", value=None)
with col3:
    z_min = st.slider("z mínimo", 0.0, 6.0, 2.0, 0.1)

vtypes = st.multiselect("Tipos de buque", options=distinct_values("VesselTypeName"))
limit = st.number_input("Límite", 50, 5000, DEFAULT_LIMIT, step=50)

params = {
    "start_date": str(start) if start else None,
    "end_date": str(end) if end else None,
    "vessel_types": vtypes,
    "z_min": float(z_min),
    "limit": int(limit),
}

sql = open("sql/calado_anomalo.sql", "r", encoding="utf-8").read()
df = run_query_df(sql, params)

st.dataframe(df, use_container_width=True)
if not df.empty:
    chart_bar(df, x="MMSI:N", y="z:Q", color="VesselTypeName:N", title="Z-score de calado por MMSI")