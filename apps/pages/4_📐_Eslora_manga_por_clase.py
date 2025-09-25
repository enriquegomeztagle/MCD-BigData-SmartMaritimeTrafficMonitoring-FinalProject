import streamlit as st
from lib.bq import run_query_df, distinct_values
from lib.ui import chart_bar

st.header("Eslora–Manga: correlación por clase")

start = st.date_input("Desde", value=None)
end = st.date_input("Hasta", value=None)
classes = st.multiselect("Clases de buque", options=distinct_values("VesselTypeClass"))
min_n = st.number_input("Mín. muestras por clase", 10, 100000, 100)

params = {
    "start_date": str(start) if start else None,
    "end_date": str(end) if end else None,
    "classes": classes,
    "min_n": int(min_n),
}

sql = open("sql/eslora_manga.sql", "r", encoding="utf-8").read()
df = run_query_df(sql, params)

st.dataframe(df, use_container_width=True)
if not df.empty:
    chart_bar(df, x="VesselTypeClass:N", y="corr_len_width:Q", title="Correlación Eslora vs Manga")