import streamlit as st
from lib.bq import run_query_df, distinct_values, get_default_dates
from lib.queries import eslora_manga_query
from lib.ui import chart_bar

st.header("Eslora-Manga: correlación por clase")

default_start, default_end = get_default_dates()

start = st.date_input("Desde", value=default_start)
end = st.date_input("Hasta", value=default_end)
classes = st.multiselect("Clases de buque", options=distinct_values("VesselTypeClass"))
min_n = st.number_input("Mín. muestras por clase", 10, 100000, 100)

sql = eslora_manga_query(start, end, classes, min_n)
df = run_query_df(sql)

st.dataframe(df, width="stretch")
if not df.empty:
    chart_bar(
        df, x="VesselTypeClass", y="corr_len_width", title="Correlación Eslora vs Manga"
    )
