import streamlit as st
import plotly.express as px
import geohash
from lib.bq import distinct_values

DEFAULT_LIMIT = 100


def chart_bar(df, x, y, title="Bar Chart", color=None):
    fig = px.bar(df, x=x, y=y, color=color, title=title)
    st.plotly_chart(fig, config={"responsive": True})


def mmsi_multiselect(label="MMSI", key=None):
    return st.multiselect(label, options=distinct_values("MMSI"), key=key)


def show_geohash_map(df, geohash_column="geohash9", title="Mapa de eventos"):
    if df.empty or geohash_column not in df.columns:
        return

    st.subheader(title)

    def decode_geohash(gh):
        try:
            return geohash.decode(gh)
        except:
            return None, None

    coords = df[geohash_column].apply(decode_geohash)
    df["lat"] = [coord[0] for coord in coords]
    df["lon"] = [coord[1] for coord in coords]

    map_df = df.dropna(subset=["lat", "lon"])

    if not map_df.empty:
        st.map(map_df[["lat", "lon"]])
    else:
        st.warning("No se pudieron decodificar las coordenadas geohash")
