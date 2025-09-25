import streamlit as st
from lib.bq import run_query_df, distinct_values, get_default_dates
from lib.queries import correlation_query
from lib.ui import chart_bar

st.header("Correlación SOG vs Draft por tipo")

default_start, default_end = get_default_dates()

start = st.date_input("Desde", value=default_start)
end = st.date_input("Hasta", value=default_end)
vtypes = st.multiselect("Tipos de buque", options=distinct_values("VesselTypeName"))
min_n = st.number_input("Mín. muestras por tipo", 10, 100000, 100)

sql = correlation_query(start, end, vtypes, "SOG", "Draft", min_n)
df = run_query_df(sql)

st.dataframe(df, width="stretch")
if not df.empty:
    chart_bar(df, x="VesselTypeName", y="corr_pearson", title="Correlación (Pearson)")
