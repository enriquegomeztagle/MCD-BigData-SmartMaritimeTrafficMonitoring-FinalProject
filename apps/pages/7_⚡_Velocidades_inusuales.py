import streamlit as st
from lib.bq import run_query_df, distinct_values, get_default_dates
from lib.queries import velocidades_inusuales_query
from lib.ui import chart_bar, DEFAULT_LIMIT

st.header("Velocidades inusuales por tipo")

default_start, default_end = get_default_dates()

start = st.date_input("Desde", value=default_start)
end = st.date_input("Hasta", value=default_end)
vtypes = st.multiselect("Tipos de buque", options=distinct_values("VesselTypeName"))
p = st.slider("Percentil p", 80, 99, 95)
limit = st.number_input("Límite", 50, 5000, DEFAULT_LIMIT, step=50)

sql = velocidades_inusuales_query(start, end, vtypes, p, limit)
df = run_query_df(sql)

st.dataframe(df, use_container_width=True)
if not df.empty:
    chart_bar(
        df,
        x="MMSI",
        y="exceso",
        color="VesselTypeName",
        title="Exceso sobre percentil p",
    )
