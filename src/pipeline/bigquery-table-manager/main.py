import json
import os
from typing import List, Dict, Any

from google.cloud import bigquery
from google.api_core.exceptions import NotFound, Conflict, BadRequest

DEFAULT_SCHEMA: List[bigquery.SchemaField] = []

_DEF_EXT_TO_SRCFMT = {
    ".parquet": bigquery.SourceFormat.PARQUET,
    ".json": bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    ".csv": bigquery.SourceFormat.CSV,
    ".avro": bigquery.SourceFormat.AVRO,
    ".orc": bigquery.SourceFormat.ORC,
}


def _infer_source_format_from_uri(uri: str):
    low = uri.lower()
    for comp in (".gz", ".bz2", ".zst"):
        if low.endswith(comp):
            low = low[: -len(comp)]
            break
    for ext, fmt in _DEF_EXT_TO_SRCFMT.items():
        if low.endswith(ext):
            return fmt
    return None


def _autodetect_schema_via_load(
    client: bigquery.Client,
    table_fqid: str,
    gcs_uri: str,
    source_format: str | None = None,
):
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_EMPTY,
    )

    if source_format:
        sf = source_format.upper()
        mapping = {
            "CSV": bigquery.SourceFormat.CSV,
            "PARQUET": bigquery.SourceFormat.PARQUET,
            "NEWLINE_DELIMITED_JSON": bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            "JSON": bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            "AVRO": bigquery.SourceFormat.AVRO,
            "ORC": bigquery.SourceFormat.ORC,
        }
        job_config.source_format = mapping.get(sf, None)
    if not getattr(job_config, "source_format", None):
        inferred = _infer_source_format_from_uri(gcs_uri)
        if inferred:
            job_config.source_format = inferred

    load_job = client.load_table_from_uri(
        gcs_uri,
        table_fqid,
        job_config=job_config,
    )
    result = load_job.result()
    return result


def _schema_from_json(schema_json: List[Dict[str, Any]]) -> List[bigquery.SchemaField]:
    def to_field(d: Dict[str, Any]) -> bigquery.SchemaField:
        name = d["name"]
        ftype = d["type"].upper()
        mode = d.get("mode", "NULLABLE").upper()
        subfields = d.get("fields")
        if subfields and isinstance(subfields, list):
            return bigquery.SchemaField(
                name=name,
                field_type=ftype,
                mode=mode,
                fields=[to_field(sd) for sd in subfields],
            )
        return bigquery.SchemaField(name=name, field_type=ftype, mode=mode)

    return [to_field(col) for col in schema_json]


def _parse_request(request) -> Dict[str, Any]:
    try:
        data = request.get_json(silent=True) or {}
    except Exception:
        data = {}

    project_id = data.get("project_id") or os.getenv("PROJECT_ID")
    dataset_id = data.get("dataset_id") or os.getenv("DATASET_ID")
    table_id = data.get("table_id") or os.getenv("TABLE_ID")

    gcs_uri = data.get("gcs_uri") or os.getenv("GCS_SAMPLE_URI")
    source_format = data.get("source_format") or os.getenv("SOURCE_FORMAT")

    schema_json = data.get("schema")
    if schema_json:
        schema = _schema_from_json(schema_json)
    else:
        schema = DEFAULT_SCHEMA

    return {
        "project_id": project_id,
        "dataset_id": dataset_id,
        "table_id": table_id,
        "schema": schema,
        "gcs_uri": gcs_uri,
        "source_format": source_format,
    }


def check_or_create_table(request):
    params = _parse_request(request)
    project_id = params["project_id"]
    dataset_id = params["dataset_id"]
    table_id = params["table_id"]
    schema = params["schema"]
    gcs_uri = params.get("gcs_uri")
    source_format = params.get("source_format")

    if not (project_id and dataset_id and table_id):
        return (
            json.dumps(
                {
                    "ok": False,
                    "error": "Faltan parámetros: project_id, dataset_id, table_id (en body o variables de entorno).",
                }
            ),
            400,
            {"Content-Type": "application/json"},
        )

    client = bigquery.Client(project=project_id)
    table_fqid = f"{project_id}.{dataset_id}.{table_id}"

    try:
        _ = client.get_table(table_fqid)
        return (
            json.dumps(
                {"ok": True, "exists": True, "action": "none", "table": table_fqid}
            ),
            200,
            {"Content-Type": "application/json"},
        )
    except NotFound:
        pass

    if not schema or len(schema) == 0:
        if gcs_uri:
            try:
                _autodetect_schema_via_load(client, table_fqid, gcs_uri, source_format)
                return (
                    json.dumps(
                        {
                            "ok": True,
                            "exists": False,
                            "action": "created (autodetect)",
                            "table": table_fqid,
                            "autodetect_from": gcs_uri,
                        }
                    ),
                    200,
                    {"Content-Type": "application/json"},
                )
            except BadRequest as e:
                return (
                    json.dumps(
                        {
                            "ok": False,
                            "exists": False,
                            "action": "failed",
                            "error": f"Autodetect falló al cargar desde {gcs_uri}: {e.message if hasattr(e, 'message') else str(e)}",
                        }
                    ),
                    400,
                    {"Content-Type": "application/json"},
                )
            except Exception as e:
                return (
                    json.dumps(
                        {
                            "ok": False,
                            "exists": False,
                            "action": "failed",
                            "error": f"Error en autodetect: {str(e)}",
                        }
                    ),
                    500,
                    {"Content-Type": "application/json"},
                )
        return (
            json.dumps(
                {
                    "ok": False,
                    "exists": False,
                    "action": "skipped",
                    "error": "La tabla no existe. Proporciona 'schema' o 'gcs_uri' para autodetectar (opcionalmente 'source_format').",
                }
            ),
            400,
            {"Content-Type": "application/json"},
        )

    try:
        table = bigquery.Table(table_fqid, schema=schema)
        created = client.create_table(table)
        return (
            json.dumps(
                {
                    "ok": True,
                    "exists": False,
                    "action": "created",
                    "table": created.full_table_id,
                }
            ),
            200,
            {"Content-Type": "application/json"},
        )
    except Conflict as e:
        return (
            json.dumps(
                {
                    "ok": True,
                    "exists": True,
                    "action": "none (race)",
                    "table": table_fqid,
                    "note": "La tabla apareció durante el intento de creación.",
                }
            ),
            200,
            {"Content-Type": "application/json"},
        )
    except BadRequest as e:
        return (
            json.dumps(
                {
                    "ok": False,
                    "exists": False,
                    "action": "failed",
                    "error": f"BadRequest al crear: {e.message if hasattr(e, 'message') else str(e)}",
                }
            ),
            400,
            {"Content-Type": "application/json"},
        )
    except Exception as e:
        return (
            json.dumps(
                {"ok": False, "exists": False, "action": "failed", "error": str(e)}
            ),
            500,
            {"Content-Type": "application/json"},
        )
