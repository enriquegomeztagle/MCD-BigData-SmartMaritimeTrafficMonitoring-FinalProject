import streamlit as st
from lib.bq import run_query_df, distinct_values
from lib.ui import chart_bar

st.header("Correlación SOG vs Draft por tipo")

start = st.date_input("Desde", value=None)
end = st.date_input("Hasta", value=None)
vtypes = st.multiselect("Tipos de buque", options=distinct_values("VesselTypeName"))
min_n = st.number_input("Mín. muestras por tipo", 10, 100000, 100)

params = {
    "start_date": str(start) if start else None,
    "end_date": str(end) if end else None,
    "vessel_types": vtypes,
    "min_n": int(min_n),
}

sql = open("sql/corr_sog_draft.sql", "r", encoding="utf-8").read()
df = run_query_df(sql, params)

st.dataframe(df, use_container_width=True)
if not df.empty:
    chart_bar(df, x="VesselTypeName:N", y="corr_pearson:Q", title="Correlación (Pearson)")