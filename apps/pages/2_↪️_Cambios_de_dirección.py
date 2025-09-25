import streamlit as st
from datetime import datetime, timedelta
from lib.bq import run_query_df
from lib.ui import mmsi_multiselect, DEFAULT_LIMIT
import pydeck as pdk

st.header("Cambios de dirección ≥ Δ (grados)")

start_date = st.date_input("Desde", value=None)
end_date   = st.date_input("Hasta", value=None)
max_dt = st.number_input("Máx. separación entre mensajes (min)", 1, 120, 10)
min_delta = st.slider("Δ rumbo mínimo (°)", 10.0, 180.0, 45.0, 1.0)

mmsi = mmsi_multiselect()
with st.expander("Filtro geográfico (opcional)"):
    lat_min = st.number_input("Lat mín", -90.0, 90.0, format="%f")
    lat_max = st.number_input("Lat máx", -90.0, 90.0, format="%f")
    lon_min = st.number_input("Lon mín", -180.0, 180.0, format="%f")
    lon_max = st.number_input("Lon máx", -180.0, 180.0, format="%f")
    use_bbox = st.checkbox("Usar bounding box")

limit = st.number_input("Límite", 50, 5000, DEFAULT_LIMIT, step=50)

params = {
    "start_date": start_date,     # <- date o None
    "end_date": end_date,         # <- date o None
    "max_dt_min": int(max_dt),
    "min_delta_deg": float(min_delta),
    "mmsi": mmsi,
    "lat_min": lat_min if use_bbox else None,
    "lat_max": lat_max if use_bbox else None,
    "lon_min": lon_min if use_bbox else None,
    "lon_max": lon_max if use_bbox else None,
    "limit": int(limit),
}

sql = open("sql/cambios_direccion.sql", "r", encoding="utf-8").read()
df = run_query_df(sql, params)

st.dataframe(df, use_container_width=True)

if not df.empty and {"LAT", "LON"}.issubset(df.columns):
    st.subheader("Mapa de eventos")
    layer = pdk.Layer(
        "ScatterplotLayer",
        data=df.rename(columns={"LAT": "lat", "LON": "lon"}),
        get_position='[lon, lat]',
        get_radius=200,
        pickable=True,
    )
    st.pydeck_chart(pdk.Deck(map_style=None, initial_view_state=pdk.ViewState(latitude=float(df.LAT.mean()), longitude=float(df.LON.mean()), zoom=5), layers=[layer]))