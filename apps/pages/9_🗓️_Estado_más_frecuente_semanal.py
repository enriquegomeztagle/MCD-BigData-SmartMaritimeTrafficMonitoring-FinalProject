import streamlit as st
from lib.bq import run_query_df, distinct_values
from lib.queries import estado_frecuente_semanal_query
import plotly.express as px

st.header("Estado más frecuente por día de la semana")

vtypes = st.multiselect("Tipos de buque", options=distinct_values("VesselTypeName"))

sql = estado_frecuente_semanal_query(vtypes)
df = run_query_df(sql)

st.dataframe(df, width='stretch')

if not df.empty:
    fig = px.bar(
        df, 
        x="dow", 
        y="count", 
        color="most_common_status",
        title="Estado más frecuente por día de la semana",
        labels={"dow": "Día", "count": "Frecuencia", "most_common_status": "Estado"}
    )
    st.plotly_chart(fig, width='stretch')
    
    # Summary by vessel type
    fig2 = px.bar(
        df, 
        x="VesselTypeName", 
        y="count", 
        color="most_common_status",
        title="Estados más frecuentes por tipo de buque",
        labels={"VesselTypeName": "Tipo de buque", "count": "Frecuencia"}
    )
    fig2.update_xaxes(tickangle=45)
    st.plotly_chart(fig2, width='stretch')
