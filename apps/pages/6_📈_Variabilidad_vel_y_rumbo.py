import streamlit as st
from lib.bq import run_query_df, distinct_values, get_default_dates
from lib.queries import variabilidad_query
from lib.ui import chart_bar

st.header("Variabilidad de velocidad y rumbo por tipo")

default_start, default_end = get_default_dates()

start = st.date_input("Desde", value=default_start)
end = st.date_input("Hasta", value=default_end)
vtypes = st.multiselect("Tipos de buque", options=distinct_values("VesselTypeName"))
min_n = st.number_input("Mín. muestras por tipo", 10, 100000, 100)

sql = variabilidad_query(start, end, vtypes, min_n)
df = run_query_df(sql)

st.dataframe(df, use_container_width=True)
if not df.empty:
    chart_bar(
        df,
        x="VesselTypeName",
        y="sd_sog",
        title="Desv. estándar SOG (↑ = más variable)",
    )
