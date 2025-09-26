"""Streamlit page for training and detecting anomalies in AIS data.

This page provides an interactive interface to train ARIMA_PLUS models on
aggregated AIS metrics and subsequently detect anomalous behaviour.  Users
can select the time range, vessel types, aggregation frequency, metric and
series identifier.  After training, they can run anomaly detection on
arbitrary date ranges.  Results are displayed as a table and visualised
through bar and scatter plots.
"""

from __future__ import annotations

import pandas as pd  # type: ignore
import streamlit as st  # type: ignore
import plotly.express as px  # type: ignore
import math
import pandas as pd
import numpy as np
from lib.bq import run_query_df, distinct_values, get_default_dates
from lib.queries import anomaly_train_query, anomaly_predict_query



"""Render the anomaly detection page."""
st.header("Detección de anomalías en tráfico marítimo")

# Obtain default dates based on the user's timezone (America/Mexico_City)
default_start, default_end = get_default_dates()
start_date, end_date = st.date_input(
    "Rango de fechas (entrenamiento y predicción)",
    value=(default_start, default_end),
    help="Selecciona el periodo sobre el que se entrenará el modelo y se evaluarán las anomalías."
)
if isinstance(start_date, tuple):
    # For Streamlit < 1.14, date_input returns a tuple
    start_date, end_date = start_date

# Vessel type selection
vessel_options = distinct_values("VesselTypeName")
vtypes = st.multiselect(
    "Tipos de buque",
    options=vessel_options,
    help="Filtra por tipo de buque.  Deja vacío para incluir todos."
)

# Metric selection: count of messages or average speed
metric_label = st.selectbox(
    "Métrica a modelar",
    options=["Número de mensajes", "Velocidad promedio"],
    help="Seleccione si desea modelar el número de mensajes o la velocidad promedio (SOG)."
)
metric_key = "count" if metric_label.startswith("Número") else "speed"

# Frequency selection
freq = st.selectbox(
    "Frecuencia de agregación",
    options=["HOURLY", "DAILY"],
    index=0,
    help="Define la granularidad de la serie temporal para el modelo ARIMA."
)

# Series identifier selection
id_col = st.selectbox(
    "Identificador de serie",
    options=["geohash9", "VesselTypeName", "MMSI"],
    index=0,
    help="La columna que define series independientes.  Se entrenará un modelo por cada valor distinto."
)

# Anomaly probability threshold
threshold = st.slider(
    "Umbral de probabilidad de anomalía", 0.80, 0.999, 0.95, 0.005,
    help="Valores más altos reducen los falsos positivos."
)

# Train model button
if st.button("Entrenar modelo"):
    with st.spinner("Ejecutando entrenamiento en BigQuery ML..."):
        try:
            train_sql = anomaly_train_query(
                start_date.isoformat(),
                end_date.isoformat(),
                vtypes,
                metric_key,
                freq=freq,
                id_col=id_col,
            )
            # Running the query will create or replace the model.  The result
            # set is empty but the call blocks until training completes.
            run_query_df(train_sql)
            st.success("Modelo entrenado correctamente.")
        except Exception as e:
            st.error(f"Error al entrenar el modelo: {e}")

# Detect anomalies button
if st.button("Detectar anomalías"):
    with st.spinner("Ejecutando detección de anomalías..."):
        try:
            predict_sql = anomaly_predict_query(
                start_date.isoformat(),
                end_date.isoformat(),
                vtypes,
                metric_key,
                freq=freq,
                id_col=id_col,
                threshold=threshold,
            )
            df = run_query_df(predict_sql)
            if df.empty:
                st.info("No se encontraron resultados para el periodo seleccionado.")
            else:
                # --- 1) Reducir datos que viajan al frontend ---
                # Qué columnas realmente necesitas para UI/plots:
                ui_cols = [c for c in ["series_id", "ts_col", "value", "is_anomaly"] if c in df.columns]
                df_ui = df[ui_cols].copy()
                # Asegurar tipos compactos
                if "is_anomaly" in df_ui.columns:
                    df_ui["is_anomaly"] = df_ui["is_anomaly"].fillna(False).astype(bool)
                if "series_id" in df_ui.columns:
                    # Opcional: esto reduce bytes al serializar
                    df_ui["series_id"] = df_ui["series_id"].astype("category")

                st.subheader("Resultados de la detección de anomalías")

                # --- 2) Tabla paginada (evita enviar todo el DF) ---
                with st.expander("Ver tabla de resultados (paginada)", expanded=False):
                    page_size = st.number_input("Filas por página", min_value=500, max_value=50_000, value=5_000, step=500)
                    total_pages = max(1, math.ceil(len(df_ui) / page_size))
                    page = st.number_input("Página", min_value=1, max_value=total_pages, value=1, step=1)
                    start = (page - 1) * page_size
                    end = start + page_size
                    st.caption(f"Mostrando {start+1:,}–{min(end, len(df_ui)):,} de {len(df_ui):,} filas")
                    st.dataframe(df_ui.iloc[start:end], use_container_width=True)
                    # Descarga: evita adjuntar CSV enorme al estado si pesa mucho
                    approx_bytes = int(df_ui.memory_usage(deep=True).sum())
                    if approx_bytes < 150 * 1024 * 1024:  # ~150MB umbral de seguridad
                        st.download_button(
                            "Descargar CSV (completo)",
                            df_ui.to_csv(index=False).encode("utf-8"),
                            "anomaly_results.csv",
                            "text/csv",
                        )
                    else:
                        st.info("El resultado es muy grande para descargar aquí. Exporta a un almacenamiento externo (S3, GCS, etc.) y comparte el link.")

                # --- 3) Gráfico de anomalías por serie (pequeño) ---
                if "is_anomaly" in df_ui.columns and "series_id" in df_ui.columns:
                    anomalies = df_ui[df_ui["is_anomaly"]]
                    if not anomalies.empty:
                        count_by_series = (
                            anomalies.groupby("series_id", dropna=False)
                            .size()
                            .reset_index(name="anomaly_count")
                            .sort_values("anomaly_count", ascending=False)
                        )
                        # limitar categorías visibles para no generar figuras gigantes
                        top_n = st.slider("Series a mostrar en la barra", 10, 200, 50, step=10)
                        fig = px.bar(
                            count_by_series.head(top_n),
                            x="series_id",
                            y="anomaly_count",
                            title="Número de anomalías por serie",
                            labels={"series_id": "Serie", "anomaly_count": "Anomalías"},
                        )
                        fig.update_layout(xaxis_tickangle=-45)
                        st.plotly_chart(fig, use_container_width=True)

                # --- 4) Scatter con muestreo y WebGL (muy importante) ---
                # Muestra SIEMPRE todos los puntos anómalos y SOLO una muestra de los normales
                if {"ts_col", "value"}.issubset(df_ui.columns):
                    df_sorted = df_ui.sort_values("ts_col")
                    mask = df_sorted["is_anomaly"].fillna(False) if "is_anomaly" in df_sorted.columns else pd.Series(False, index=df_sorted.index)
                    anomalies = df_sorted[mask]
                    normals = df_sorted[~mask]

                    max_normals = st.slider("Puntos normales máximos en el gráfico", 5_000, 200_000, 50_000, step=5_000)
                    if len(normals) > max_normals:
                        normals = normals.sample(max_normals, random_state=42)

                    plot_df = pd.concat([normals, anomalies], ignore_index=True)
                    # Etiquetas legibles para color
                    estado = plot_df["is_anomaly"].fillna(False) if "is_anomaly" in plot_df.columns else pd.Series(False, index=plot_df.index)
                    plot_df["Estado"] = np.where(estado, "Anomalía", "Normal")

                    fig2 = px.scatter(
                        plot_df,
                        x="ts_col",
                        y="value",
                        color="Estado",
                        title="Serie temporal con anomalías destacadas",
                        labels={"ts_col": "Fecha", "value": "Valor"},
                        render_mode="webgl",  # WebGL -> más eficiente para muchos puntos
                    )
                    fig2.update_traces(marker=dict(size=6))
                    st.plotly_chart(fig2, use_container_width=True) 
        except Exception as e:
            st.error(f"Error al detectar anomalías: {e}")
