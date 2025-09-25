from __future__ import annotations
import streamlit as st
import pandas as pd
import altair as alt

# Defaults comunes
DEFAULT_LIMIT = 200


def mmsi_multiselect(key: str = "mmsi") -> list[int]:
    txt = st.text_input("MMSI (coma‑separados)", key=key, placeholder="e.g., 351234000, 224567890")
    if not txt.strip():
        return []
    vals = []
    for token in txt.replace("\n", ",").split(","):
        token = token.strip()
        if token.isdigit():
            vals.append(int(token))
    return vals


def chart_bar(df: pd.DataFrame, x: str, y: str, color: str | None = None, title: str | None = None):
    chart = alt.Chart(df).mark_bar().encode(
        x=alt.X(x, sort='-y'),
        y=y,
        color=color
    )
    if title:
        chart = chart.properties(title=title)
    st.altair_chart(chart, use_container_width=True)


def chart_scatter(df: pd.DataFrame, x: str, y: str, color: str | None = None, size: str | None = None, title: str | None = None):
    chart = alt.Chart(df).mark_circle(opacity=0.6).encode(
        x=x,
        y=y,
        color=color if color else alt.value("#1f77b4"),
        size=size if size else alt.value(40),
        tooltip=list(df.columns)
    )
    if title:
        chart = chart.properties(title=title)
    st.altair_chart(chart, use_container_width=True)