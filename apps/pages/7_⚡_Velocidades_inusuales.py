import streamlit as st
from lib.bq import run_query_df, distinct_values
from lib.ui import chart_bar, DEFAULT_LIMIT

st.header("Velocidades inusuales por tipo")

start = st.date_input("Desde", value=None)
end = st.date_input("Hasta", value=None)
vtypes = st.multiselect("Tipos de buque", options=distinct_values("VesselTypeName"))
p = st.slider("Percentil p", 80, 99, 95)
limit = st.number_input("Límite", 50, 5000, DEFAULT_LIMIT, step=50)

params = {
    "start_date": str(start) if start else None,
    "end_date": str(end) if end else None,
    "vessel_types": vtypes,
    "p": int(p),
    "limit": int(limit),
}

sql = open("sql/velocidades_inusuales.sql", "r", encoding="utf-8").read()
df = run_query_df(sql, params)

st.dataframe(df, use_container_width=True)
if not df.empty:
    df_plot = df.copy()
    df_plot["exceso"] = df_plot["sog_max"] - df_plot["sog_p"]
    chart_bar(df_plot, x="MMSI:N", y="exceso:Q", color="VesselTypeName:N", title="Exceso sobre percentil p")