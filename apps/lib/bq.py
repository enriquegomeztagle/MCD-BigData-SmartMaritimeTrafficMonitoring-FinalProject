from __future__ import annotations
from typing import Any, Dict, Optional
from pathlib import Path
import json
import os
import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import date, datetime

def _resolve_keyfile_path(explicit_path: Optional[str]) -> Path:
    """
    Orden de resolución:
      1) Parámetro explícito `explicit_path`
      2) st.secrets["GCP_KEYFILE_PATH"]  (o "gcp_keyfile_path")
      3) env var GOOGLE_APPLICATION_CREDENTIALS
    """
    candidates: list[str] = []
    if explicit_path:
        candidates.append(explicit_path)

    # Permitimos dos claves, por si cambias el nombre
    for k in ("GCP_KEYFILE_PATH", "gcp_keyfile_path"):
        if k in st.secrets:
            candidates.append(str(st.secrets[k]))

    if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
        candidates.append(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])

    # Tomamos el primer path existente
    for c in candidates:
        p = Path(c).expanduser()
        if p.exists() and p.is_file():
            return p

    # Si llegamos aquí, no hay path válido
    hints = [
        "1) pásalo como argumento: get_bq_client('C:/ruta/clave.json')",
        "2) define en secrets.toml: GCP_KEYFILE_PATH = 'C:/ruta/clave.json'",
        "3) exporta la env var GOOGLE_APPLICATION_CREDENTIALS con el path a la clave",
    ]
    raise FileNotFoundError(
        "No se encontró un JSON de servicio. Configura un path válido.\n" + "\n".join(hints)
    )


def _project_from_keyfile(path: Path) -> Optional[str]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return (json.load(f) or {}).get("project_id")
    except Exception:
        return None


@st.cache_resource(show_spinner=False)
def get_bq_client(keyfile_path: Optional[str] = None) -> bigquery.Client:
    """
    Autenticación SIEMPRE por archivo JSON en disco.
    Cachea el cliente por combinación de path.
    """
    key_path = _resolve_keyfile_path(keyfile_path)
    credentials = service_account.Credentials.from_service_account_file(str(key_path))

    # Intenta obtener project_id del JSON o de env vars estándar.
    project = (
        _project_from_keyfile(key_path)
        or os.getenv("GCP_PROJECT")
        or os.getenv("GOOGLE_CLOUD_PROJECT")
        or getattr(credentials, "project_id", None)
    )
    if not project:
        raise RuntimeError(
            "No se pudo determinar el project_id. Asegúrate de que el JSON tenga `project_id` "
            "o define GCP_PROJECT / GOOGLE_CLOUD_PROJECT."
        )

    return bigquery.Client(credentials=credentials, project=project)


def _bq_param(name, value):
    from google.cloud.bigquery import ScalarQueryParameter, ArrayQueryParameter
    if isinstance(value, list):
        if not value: return ArrayQueryParameter(name, "STRING", [])
        first = value[0]
        if isinstance(first, int):   return ArrayQueryParameter(name, "INT64", value)
        if isinstance(first, float): return ArrayQueryParameter(name, "FLOAT64", value)
        return ArrayQueryParameter(name, "STRING", value)

    # Mantén el tipo correcto aunque el valor sea None
    if value is None:
        if name.endswith("_date"):      return ScalarQueryParameter(name, "DATE", None)
        if name.endswith("_ts") or name.endswith("_timestamp"):
                                        return ScalarQueryParameter(name, "TIMESTAMP", None)
        if name.startswith(("lat_", "lon_")):
                                        return ScalarQueryParameter(name, "FLOAT64", None)
        return ScalarQueryParameter(name, "STRING", None)

    if isinstance(value, bool):      return ScalarQueryParameter(name, "BOOL", value)
    if isinstance(value, int):       return ScalarQueryParameter(name, "INT64", value)
    if isinstance(value, float):     return ScalarQueryParameter(name, "FLOAT64", value)
    if isinstance(value, date) and not isinstance(value, datetime):
                                     return ScalarQueryParameter(name, "DATE", value)
    if isinstance(value, datetime):  return ScalarQueryParameter(name, "TIMESTAMP", value)
    return ScalarQueryParameter(name, "STRING", value)


def run_query_df(sql: str, params: Dict[str, Any] | None = None, table: str | None = None) -> pd.DataFrame:
    client = get_bq_client()

    # Reemplazo de placeholder de tabla
    table = table or st.secrets.get("BQ_TABLE") or os.getenv("BQ_TABLE")

    if table:
        # admite {{TABLE}} y {TABLE}
        sql = sql.replace("{{TABLE}}", table).replace("{TABLE}", table)

    # Aviso temprano si quedó sin reemplazar
    if "{{TABLE}}" in sql or "{TABLE}" in sql:
        raise ValueError(
            "El placeholder de tabla no fue reemplazado. Define BQ_TABLE (p.ej. 'dataset.tabla' o 'proyecto.dataset.tabla') "
            "en st.secrets, variables de entorno, o pásalo vía run_query_df(..., table='...')."
        )

    job_config = None
    if params:
        job_config = bigquery.QueryJobConfig(
            query_parameters=[_bq_param(k, v) for k, v in params.items()]
        )

    job = client.query(sql, job_config=job_config)
    return job.result().to_dataframe(create_bqstorage_client=False)



@st.cache_data(show_spinner=False)
def distinct_values(column: str, limit: int = 200) -> list[str]:
    sql = f"""
        SELECT DISTINCT {column} AS v
        FROM `{{{{TABLE}}}}`
        WHERE {column} IS NOT NULL
        ORDER BY v
        LIMIT {limit}
    """
    df = run_query_df(sql)
    return df["v"].dropna().astype(str).tolist()
