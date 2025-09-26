from __future__ import annotations
import streamlit as st
import os
import streamlit as st
from google.cloud import bigquery

def _qualify(name: str) -> str:
    """
    Devuelve project.dataset.obj a partir de:
      - project.dataset.obj  (ya calificado)
      - dataset.obj          (usa BQ_PROJECT o el del cliente)
    """
    name = name.strip()
    parts = name.split(".")
    if len(parts) == 3:
        return name
    if len(parts) == 2:
        project = st.secrets.get("BQ_PROJECT") or os.getenv("BQ_PROJECT")
        if not project:
            # Toma el proyecto por defecto del cliente si no viene en secrets/env
            client = bigquery.Client()
            project = client.project
        return f"{project}.{name}"
    raise ValueError(f"Nombre inválido '{name}'. Esperado 'project.dataset.object'.")

def get_table_name() -> str:
    raw = st.secrets.get(
        "BQ_TABLE",
        os.getenv("BQ_TABLE", "river-treat-468823.ais_data.ais_messages"),
    )
    return _qualify(raw)

def get_project_id() -> str:
    # Del secrets/env o del cliente
    project = st.secrets.get("BQ_PROJECT") or os.getenv("BQ_PROJECT")
    if project:
        return project
    client = bigquery.Client()
    return client.project

def get_model_dataset() -> str:
    """Dataset donde se crearán los modelos de BQML."""
    raw = st.secrets.get("BQ_MODEL_DATASET")
    if raw:
        # Puede venir como solo dataset => califica contra el project
        proj = get_project_id()
        return f"{proj}.{raw}" if "." not in raw else f"{proj}.{raw.split('.')[0]}" if raw.count(".")==0 else raw
    # Por defecto, usa el mismo dataset de la tabla fuente
    project, dataset, _ = get_table_name().split(".")
    return f"{project}.{dataset}"

def get_results_table_name() -> str:
    raw = st.secrets.get("BQ_RESULTS_TABLE")
    if raw:
        return _qualify(raw)
    # Por defecto, misma localización que la tabla fuente + nombre anomaly_results
    project, dataset, _ = get_table_name().split(".")
    return f"{project}.{dataset}.anomaly_results"


def build_date_filter(start_date, end_date, date_column="BaseDateTime"):
    return f"AND DATE({date_column}) >= '{start_date}' AND DATE({date_column}) <= '{end_date}'"


def build_vessel_filter(vessel_types, column="VesselTypeName"):
    if not vessel_types:
        return ""
    normalized_types = [("None" if vt is None else str(vt)) for vt in vessel_types]
    vessel_list = "', '".join(normalized_types)
    return f"AND {column} IN ('{vessel_list}')"


def build_mmsi_filter(mmsi_list):
    if not mmsi_list:
        return ""
    mmsi_str = "', '".join(map(str, mmsi_list))
    return f"AND MMSI IN ('{mmsi_str}')"

def build_bbox_filter(lat_min: float, lat_max: float, lon_min: float, lon_max: float, use_bbox: bool = True) -> str:
    """Create a bounding box filter for latitude and longitude.

    Parameters
    ----------
    lat_min : float
        Minimum latitude.
    lat_max : float
        Maximum latitude.
    lon_min : float
        Minimum longitude.
    lon_max : float
        Maximum longitude.
    use_bbox : bool, optional
        When ``True``, the filter is applied; when ``False``, returns an
        empty string, by default ``True``.

    Returns
    -------
    str
        A fragment of SQL beginning with ``AND`` that constrains ``LAT`` and
        ``LON`` within the provided bounds, or an empty string if
        ``use_bbox`` is ``False``.
    """
    if not use_bbox:
        return ""
    return f" AND LAT BETWEEN {lat_min} AND {lat_max} AND LON BETWEEN {lon_min} AND {lon_max}"