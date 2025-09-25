import streamlit as st
from lib.bq import run_query_df, get_default_dates
from lib.queries import cambios_direccion_query
from lib.query_utils import build_bbox_filter
from lib.ui import mmsi_multiselect, DEFAULT_LIMIT, show_geohash_map

st.header("Cambios de dirección ≥ Δ (grados)")

default_start, default_end = get_default_dates()

start_date = st.date_input("Desde", value=default_start)
end_date = st.date_input("Hasta", value=default_end)
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

bbox_filter = build_bbox_filter(lat_min, lat_max, lon_min, lon_max, use_bbox)
sql = cambios_direccion_query(start_date, end_date, mmsi, min_delta, bbox_filter, limit)
df = run_query_df(sql)

st.dataframe(df, use_container_width=True)
show_geohash_map(df)
