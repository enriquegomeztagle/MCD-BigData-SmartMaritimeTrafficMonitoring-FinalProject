import streamlit as st
from lib.bq import run_query_df, distinct_values
from lib.queries import velocidad_dia_semana_query
import plotly.express as px

st.header("Velocidad promedio por día de la semana")

vtypes = st.multiselect("Tipos de buque", options=distinct_values("VesselTypeName"))

sql = velocidad_dia_semana_query(vtypes)
df = run_query_df(sql)

st.dataframe(df, width="stretch")

if not df.empty:
    fig = px.bar(
        df,
        x="dow",
        y="avg_sog",
        color="VesselTypeName",
        title="Velocidad promedio por día de la semana",
        labels={"dow": "Día", "avg_sog": "SOG promedio"},
    )
    st.plotly_chart(fig, width="stretch")

    fig2 = px.line(
        df,
        x="dow",
        y="avg_sog",
        color="VesselTypeName",
        title="Tendencia semanal de velocidad",
    )
    st.plotly_chart(fig2, width="stretch")
