# MCD-BigData-SmartMaritimeTrafficMonitoring-FinalProject

End-to-end project for Smart Maritime Traffic Monitoring using NOAA AIS 2024 data. It includes:

- A Streamlit dashboard backed by Google BigQuery.
- PySpark data pipelines for raw ingestion and curated transformations stored in GCS.
- BigQuery table utilities and a lightweight table manager.
- A written report with figures documenting architecture, queries, and results.

This README focuses on what the repository contains and how the pieces fit together.

## High-level architecture

- **Data source**: NOAA AIS ZIPs (monthly, daily bundles) from `coast.noaa.gov`.
- **Ingestion**: `src/pipeline/raw` unzips to GCS and writes partitioned Parquet.
- **Curation**: `src/pipeline/curated` cleans/enriches AIS, normalizes fields, adds features (geohash9, temporal derives), and writes curated Parquet.
- **Serving**: Curated data is loaded/queried in BigQuery. Streamlit app queries BigQuery for interactive analysis and anomaly detection with BigQuery ML.
- **Documentation**: `docs/paper` includes the report and figures.

---

## Repository layout and contents

### apps/ — Streamlit dashboard

- `apps/app.py`: Streamlit entrypoint. Sets up page config and a landing page. The sidebar discovers pages under `apps/pages/` automatically.
- `apps/requirements.txt`: Python packages for the Streamlit app (Streamlit, BigQuery client, pandas, pyarrow, geohash, etc.).
- `apps/credentials.json`: Example GCP service account key. Handle securely.
- `apps/sql/*.sql`: Reference SQL files used for analytics (mirrors logic in code). Files include:
  - `avg_speed_deviation_day_week_vesseltypename.sql`
  - `calado_anomalo.sql`
  - `cambios_direccion.sql`
  - `corr_sog_draft.sql`
  - `eslora_manga.sql`
  - `incoherencias_estado.sql`
  - `most_frequent_state_by_week.sql`
  - `resumen_estado.sql`
  - `variabilidad_vel_rumbo.sql`
  - `velocidades_inusuales.sql`

#### apps/lib — App utilities

- `apps/lib/bq.py`
  - Configures BigQuery access (reads `GCP_KEYFILE_PATH` from `st.secrets`).
  - `run_query_df(query, params=None)`: executes SQL and returns `pandas.DataFrame`.
  - `distinct_values(column, table=None)`: fetches distinct values for UI filters.
  - `get_default_dates()`: returns a default date range centered on 2024 for convenient queries.
- `apps/lib/query_utils.py`
  - Helpers to resolve fully-qualified table names based on `st.secrets`/env (`BQ_TABLE`, `BQ_PROJECT`).
  - Builders for common SQL filters: `build_date_filter`, `build_vessel_filter`, `build_mmsi_filter`, `build_bbox_filter`.
  - Model dataset/table helpers: `get_model_dataset()`, `get_results_table_name()`.
- `apps/lib/queries.py`
  - Central place for parameterized SQL string builders used by pages.
  - Key functions:
    - `calado_anomalo_query(...)`: z-score deviations of average draft by vessel type.
    - `cambios_direccion_query(...)`: heading change detections using `geohash9` and window functions.
    - `correlation_query(...)`: Pearson correlation across metrics (e.g., SOG vs Draft) by vessel type.
    - `eslora_manga_query(...)`: correlation between length and width by class.
    - `resumen_estado_query(...)`: counts, unique MMSI, and averages by navigation status.
    - `variabilidad_query(...)`: variability of speed/course by vessel type.
    - `velocidades_inusuales_query(...)`: per-vessel speed outliers via quantiles.
    - `velocidad_dia_semana_query(...)`: average speed by weekday.
    - `estado_frecuente_semanal_query(...)`: most common navigation status by weekday.
    - BigQuery ML anomaly detection:
      - `anomaly_train_query(...)`: creates or replaces ARIMA_PLUS models per ID series.
      - `anomaly_predict_query(...)`: calls `ML.DETECT_ANOMALIES` and returns anomalies ordered by probability.
- `apps/lib/ui.py`
  - UI helpers: `chart_bar` using Plotly Express; `mmsi_multiselect` bound to BigQuery; `show_geohash_map` to decode `geohash9` and display a map in Streamlit.

#### apps/pages — Individual analysis pages

Each Streamlit page focuses on an analysis backed by queries in `apps/lib/queries.py`:

- `1_🛳️_Calado_anómalo.py`: anomalous draft per vessel type.
- `2_↪️_Cambios_de_dirección.py`: heading change events and mapping via `geohash9`.
- `3_⤳_Correlación_SOG_vs_Draft.py`: SOG vs Draft correlations.
- `4_📐_Eslora_manga_por_clase.py`: length-width correlations by class.
- `5_📅_Resumen_estado_+_incoherencias.py`: navigation status summaries and consistency checks.
- `6_📈_Variabilidad_vel_y_rumbo.py`: variability analysis for SOG/COG.
- `7_⚡_Velocidades_inusuales.py`: unusual speeds by vessel.
- `8_📊_Velocidad_por_día_semana.py`: speed distributions by weekday.
- `9_🗓️_Estado_más_frecuente_semanal.py`: most frequent status by weekday.
- `10_📡_Detección_de_anomalías_AIS.py`: anomaly training/inference (BigQuery ML).

### src/ — Data pipelines and utilities

#### src/pipeline/scraping

- `src/pipeline/scraping/scrapper.py`
  - Scrapes NOAA AIS index `INDEX_URL` for monthly ZIPs.
  - Multi-threaded downloads with retries, size checks, and progress.
  - Writes files to local `OUTPUT_DIR`.

#### src/pipeline/raw

- `src/pipeline/raw/raw_ingest_zip_monthly.py`
  - PySpark job to ingest monthly NOAA ZIPs from GCS to Parquet.
  - Steps:
    1) List ZIPs in `gs://<BUCKET>/<ZIP_PREFIX>` filtered by `ZIP_NAME_REGEX`.
    2) Parallel unzip to `OUT_UNZIPPED_PREFIX` in GCS.
    3) Read CSVs with an explicit schema; add ingest metadata.
    4) Write partitioned Parquet by `ym` to `OUT_PARQUET_PREFIX`.
    5) Validate CSV vs Parquet row counts per day; fail if mismatches.
    6) Clean temp unzipped files (configurable) and stop Spark.
  - Parameters via CLI or env: `PROJECT_ID`, `BUCKET`, `ZIP_PREFIX`, `OUT_UNZIPPED_PREFIX`, `OUT_PARQUET_PREFIX`, `CSV_OPTS`, `CLEANUP_UNZIPPED`, `ZIP_NAME_REGEX`.

#### src/pipeline/curated

- `src/pipeline/curated/curated_transformations_gradual_writer.py`
  - PySpark job that reads `INPUT_BASE` Parquet partitions (`ym=YYYY-MM`), applies curated transformations, and writes to `OUTPUT_BASE` partitioned by `ym`.
  - Highlights of `apply_curated_transformations`:
    - Type casting, trimming, normalization; MMSI standardization; timestamp parsing.
    - Coordinate cleaning and longitude wrapping; range rules for SOG/COG/Heading/Length/Width/Draft.
    - Map AIS numeric vessel types to human-readable `VesselTypeName` and grouped `VesselTypeClass`.
    - Navigation status enrichment (`NavStatusInt` → `NavStatusName`).
    - Text normalization of `VesselName` and `CallSign`.
    - Temporal derives: `ym`, `date`, `hour`, `dow`, `week`, `month`, `quarter`, and `SOG_ms`.
    - Geospatial feature: `geohash9` via vectorized UDF.
    - Final deduplication by `(MMSI, BaseDateTime)`.
  - Operational controls: `MONTHS`, `SAVE_MODE`, `TARGET_FILES_PER_PARTITION`, `SHUFFLE_PARTITIONS`, resume markers (`_markers/ym=.../_SUCCESS`), and existence checks.

#### src/pipeline/bigquery-table-manager

- `src/pipeline/bigquery-table-manager/main.py`
  - HTTP handler (suitable for Cloud Functions/Run) that ensures a BigQuery table exists.
  - If the table is missing and no schema is provided, can auto-detect by loading a small sample from GCS (`gcs_uri`).
  - Returns structured JSON with `ok`, `exists`, and `action` fields.
  - Useful for provisioning or validating raw/curated tables.

### docs/ — Report and figures

- `docs/paper/main.pdf`: compiled report.
- `docs/paper/main.tex`: LaTeX source.
- `docs/paper/figures/`: architecture and analysis figures used in the report (e.g., `ArchitectureDiagram.png`, `QuerysBQ.png`, `ModelosBQ.png`, and analysis plots like `VelPromDiaria.jpeg`).
- `docs/PDFs/`: exported project PDF deliverables.

## Configuration

The Streamlit app uses `st.secrets`  and environment variables for BigQuery/GCP configuration:

- `GCP_KEYFILE_PATH`: path to the GCP JSON keyfile.
- `BQ_PROJECT`: default GCP project (if not derived from credentials).
- `BQ_TABLE`: fully qualified or partially qualified table for AIS messages.
- `BQ_MODEL_DATASET`: dataset for BigQuery ML models. Defaults to the dataset of `BQ_TABLE` if not set.
- `BQ_RESULTS_TABLE`: table for anomaly results. Defaults to `<project>.<dataset>.anomaly_results`.

Spark pipelines read configuration from constants and environment variables within each script, with optional CLI overrides.

## Running components (minimal)

- Streamlit dashboard:

  1. Install `apps/requirements.txt` in a virtualenv.
  2. Provide credentials via `st.secrets` or `GCP_KEYFILE_PATH`.
  3. Run: `streamlit run apps/app.py`
- Pipelines:

  - Submit the PySpark scripts to your cluster (Dataproc/YARN/EMR/local) with appropriate env vars and access to GCS. Review the configurable constants at the top of each script.

## Data model notes

- Core columns used across analyses: `MMSI`, `BaseDateTime`, `LAT`, `LON`, `SOG`, `COG`, `Heading`, `VesselType(Int/Name/Class)`, `Length`, `Width`, `Draft`, `NavStatus(Int/Name)`, `geohash9`, and temporal partitions (`ym`).
- Analyses aggregate by vessel type/class, time windows (hour/day/week), and location cells (`geohash9`).
- Anomaly detection trains ARIMA_PLUS models per series (`series_id` configurable: `geohash9`, `VesselTypeName`, or `MMSI`) on either counts or average speed.

## Security and data handling

- Do not commit real service account keys. Use `st.secrets` or environment variables.
- GCS URIs and bucket names in scripts are examples; parameterize for production.
- Validate data before promotion: the raw ingest job includes per-day CSV vs Parquet count checks.

## Status

This repository is a working prototype that covers scraping → raw → curated → BigQuery → dashboard, with accompanying documentation and example SQL. Adapt bucket names, datasets, and credentials to your environment.
