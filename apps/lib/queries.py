from .query_utils import (
    get_table_name,
    build_date_filter,
    build_vessel_filter,
    build_mmsi_filter,
    get_model_dataset
)


def calado_anomalo_query(start_date, end_date, vessel_types, z_min, limit):
    table = get_table_name()
    date_filter = build_date_filter(start_date, end_date)
    vessel_filter = build_vessel_filter(vessel_types)

    return f"""
    WITH per_vessel AS (
    SELECT
    VesselTypeName AS vt,
    MMSI,
    AVG(CAST(Draft AS FLOAT64)) AS avg_draft
    FROM `{table}`
    WHERE Draft IS NOT NULL
    {date_filter}
    {vessel_filter}
    GROUP BY vt, MMSI
    ),
    stats AS (
    SELECT
    vt,
    AVG(avg_draft) AS mu,
    STDDEV_SAMP(avg_draft) AS sd
    FROM per_vessel
    GROUP BY vt
    )
    SELECT
    p.vt AS VesselTypeName,
    p.MMSI,
    p.avg_draft,
    s.mu,
    s.sd,
    ABS(p.avg_draft - s.mu) / NULLIF(s.sd, 0) AS z
    FROM per_vessel p
    JOIN stats s USING (vt)
    WHERE s.sd > 0
    AND ABS(p.avg_draft - s.mu) / s.sd >= {z_min}
    ORDER BY z DESC
    LIMIT {limit}
    """


def cambios_direccion_query(
    start_date, end_date, mmsi_list, min_delta, bbox_filter, limit
):
    """Generate cambios de dirección query using geohash"""
    table = get_table_name()
    date_filter = build_date_filter(start_date, end_date)
    mmsi_filter = build_mmsi_filter(mmsi_list)

    return f"""
    SELECT 
      MMSI,
      BaseDateTime,
      geohash9,
      COG,
      LAG(COG) OVER (PARTITION BY MMSI ORDER BY BaseDateTime) AS prev_cog,
      ABS(COG - LAG(COG) OVER (PARTITION BY MMSI ORDER BY BaseDateTime)) AS delta_cog
    FROM `{table}`
    WHERE COG IS NOT NULL
      AND geohash9 IS NOT NULL
    {date_filter}
    {mmsi_filter}
    {bbox_filter}
    QUALIFY ABS(COG - LAG(COG) OVER (PARTITION BY MMSI ORDER BY BaseDateTime)) >= {min_delta}
    ORDER BY delta_cog DESC
    LIMIT {limit}
    """


def correlation_query(start_date, end_date, vessel_types, col1, col2, min_n):
    """Generate correlation query"""
    table = get_table_name()
    date_filter = build_date_filter(start_date, end_date)
    vessel_filter = build_vessel_filter(vessel_types)

    return f"""
    WITH data AS (
    SELECT 
      VesselTypeName,
      CAST({col1} AS FLOAT64) AS x,
      CAST({col2} AS FLOAT64) AS y
    FROM `{table}`
    WHERE {col1} IS NOT NULL 
      AND {col2} IS NOT NULL
    {date_filter}
    {vessel_filter}
    )
    SELECT 
      VesselTypeName,
      COUNT(*) AS n,
      CORR(x, y) AS corr_pearson
    FROM data
    GROUP BY VesselTypeName
    HAVING COUNT(*) >= {min_n}
    ORDER BY ABS(corr_pearson) DESC
    """


def location_query(
    start_date, end_date, vessel_types, mmsi_list, limit, additional_columns=""
):
    """Generate generic location query with geohash"""
    table = get_table_name()
    date_filter = build_date_filter(start_date, end_date)
    vessel_filter = build_vessel_filter(vessel_types)
    mmsi_filter = build_mmsi_filter(mmsi_list)

    return f"""
    SELECT 
      MMSI,
      BaseDateTime,
      geohash9,
      VesselTypeName
      {additional_columns}
    FROM `{table}`
    WHERE geohash9 IS NOT NULL
    {date_filter}
    {vessel_filter}
    {mmsi_filter}
    ORDER BY BaseDateTime DESC
    LIMIT {limit}
    """
def eslora_manga_query(start_date, end_date, classes, min_n):
    """Generate eslora-manga correlation query"""
    table = get_table_name()
    date_filter = build_date_filter(start_date, end_date)
    
    class_filter = ""
    if classes:
        class_list = "', '".join(classes)
        class_filter = f"AND VesselTypeClass IN ('{class_list}')"
    
    return f"""
    WITH data AS (
    SELECT 
      VesselTypeClass,
      CAST(Length AS FLOAT64) AS length,
      CAST(Width AS FLOAT64) AS width
    FROM `{table}`
    WHERE Length IS NOT NULL 
      AND Width IS NOT NULL
    {date_filter}
    {class_filter}
    )
    SELECT 
      VesselTypeClass,
      COUNT(*) AS n,
      CORR(length, width) AS corr_len_width
    FROM data
    GROUP BY VesselTypeClass
    HAVING COUNT(*) >= {min_n}
    ORDER BY ABS(corr_len_width) DESC
    """

def resumen_estado_query(start_date, end_date, nav_status):
    """Generate resumen estado query"""
    table = get_table_name()
    date_filter = build_date_filter(start_date, end_date)
    
    status_filter = ""
    if nav_status:
        status_list = "', '".join(nav_status)
        status_filter = f"AND NavStatusName IN ('{status_list}')"
    
    return f"""
    SELECT 
      NavStatusName,
      COUNT(*) AS total_messages,
      COUNT(DISTINCT MMSI) AS unique_vessels,
      AVG(CAST(SOG AS FLOAT64)) AS avg_sog,
      AVG(CAST(Draft AS FLOAT64)) AS avg_draft
    FROM `{table}`
    WHERE NavStatusName IS NOT NULL
    {date_filter}
    {status_filter}
    GROUP BY NavStatusName
    ORDER BY total_messages DESC
    """

def variabilidad_query(start_date, end_date, vessel_types, min_n):
    """Generate variabilidad velocidad y rumbo query"""
    table = get_table_name()
    date_filter = build_date_filter(start_date, end_date)
    vessel_filter = build_vessel_filter(vessel_types)
    
    return f"""
    SELECT 
      VesselTypeName,
      COUNT(*) AS n,
      STDDEV(CAST(SOG AS FLOAT64)) AS sd_sog,
      STDDEV(CAST(COG AS FLOAT64)) AS sd_cog,
      AVG(CAST(SOG AS FLOAT64)) AS avg_sog,
      AVG(CAST(COG AS FLOAT64)) AS avg_cog
    FROM `{table}`
    WHERE SOG IS NOT NULL 
      AND COG IS NOT NULL
    {date_filter}
    {vessel_filter}
    GROUP BY VesselTypeName
    HAVING COUNT(*) >= {min_n}
    ORDER BY sd_sog DESC
    """

def velocidades_inusuales_query(start_date, end_date, vessel_types, percentile, limit):
    """Generate velocidades inusuales query"""
    table = get_table_name()
    date_filter = build_date_filter(start_date, end_date)
    vessel_filter = build_vessel_filter(vessel_types)
    
    return f"""
    WITH vessel_stats AS (
    SELECT 
      MMSI,
      VesselTypeName,
      MAX(CAST(SOG AS FLOAT64)) AS sog_max,
      APPROX_QUANTILES(CAST(SOG AS FLOAT64), 100)[OFFSET({percentile})] AS sog_p
    FROM `{table}`
    WHERE SOG IS NOT NULL
    {date_filter}
    {vessel_filter}
    GROUP BY MMSI, VesselTypeName
    )
    SELECT 
      MMSI,
      VesselTypeName,
      sog_max,
      sog_p,
      sog_max - sog_p AS exceso
    FROM vessel_stats
    WHERE sog_max > sog_p
    ORDER BY exceso DESC
    LIMIT {limit}
    """

def velocidad_dia_semana_query(vessel_types):
    """Generate velocidad por día de la semana query"""
    table = get_table_name()
    vessel_filter = build_vessel_filter(vessel_types)
    
    return f"""
    WITH t AS (
      SELECT
        VesselTypeName,
        FORMAT_DATE('%A', DATE(BaseDateTime)) AS dow,
        EXTRACT(DAYOFWEEK FROM DATE(BaseDateTime)) AS dow_sun1,
        SOG
      FROM `{table}`
      WHERE VesselTypeName IS NOT NULL
      {vessel_filter}
    )
    SELECT
      VesselTypeName,
      dow,
      AVG(SOG) AS avg_sog,
      STDDEV_SAMP(SOG) AS sd_sog,
      COUNT(*) AS n
    FROM t
    GROUP BY VesselTypeName, dow, dow_sun1
    ORDER BY
      VesselTypeName,
      (MOD(dow_sun1 + 5, 7) + 1)
    """

def estado_frecuente_semanal_query(vessel_types):
    """Generate estado más frecuente por día de la semana query"""
    table = get_table_name()
    vessel_filter = build_vessel_filter(vessel_types)
    
    return f"""
    WITH t AS (
      SELECT
        VesselTypeName,
        NavStatusName,
        FORMAT_DATE('%A', DATE(BaseDateTime)) AS dow,
        EXTRACT(DAYOFWEEK FROM DATE(BaseDateTime)) AS dow_sun1
      FROM `{table}`
      WHERE VesselTypeName IS NOT NULL AND NavStatusName IS NOT NULL
      {vessel_filter}
    ),
    counts AS (
      SELECT
        VesselTypeName,
        dow,
        dow_sun1,
        NavStatusName,
        COUNT(*) AS c
      FROM t
      GROUP BY VesselTypeName, dow, dow_sun1, NavStatusName
    ),
    ranked AS (
      SELECT
        VesselTypeName,
        dow,
        dow_sun1,
        NavStatusName,
        c,
        ROW_NUMBER() OVER (PARTITION BY VesselTypeName, dow ORDER BY c DESC) AS rn
      FROM counts
    )
    SELECT
      VesselTypeName,
      dow,
      NavStatusName AS most_common_status,
      c AS count
    FROM ranked
    WHERE rn = 1
    ORDER BY
      VesselTypeName,
      (MOD(dow_sun1 + 5, 7) + 1)
    """


# --- Anomaly detection builders ------------------------------------------------


def _get_model_name(metric: str, id_col: str, freq: str) -> str:
    """
    Nombre completamente calificado del modelo en el dataset definido.
    """
    def sanitize(name: str) -> str:
        return ''.join(ch.lower() if ch.isalnum() else '_' for ch in name)

    model_ds = get_model_dataset()  # e.g. river-treat-468823.dataset_AIS_2024_silver_refined
    model_name = f"anomaly_{sanitize(metric)}_{sanitize(id_col)}_{sanitize(freq)}"
    return f"{model_ds}.{model_name}"


def anomaly_train_query(
    start_date,
    end_date,
    vessel_types,
    metric,
    freq = "HOURLY",
    id_col = "geohash9",
    horizon = 1,
) -> str:
    """Build a BigQuery ML query to train an ARIMA_PLUS anomaly model.

    Parameters
    ----------
    start_date, end_date : str
        Inclusive date range used to aggregate training data.
    vessel_types : list[str] | None
        Optional list of vessel types to filter on.
    metric : str
        Metric to model.  Accepted values are ``'count'`` for message
        counts and ``'speed'`` for average speed.
    freq : str, optional
        Data frequency (``'HOURLY'`` or ``'DAILY'``).  Determines
        ``TIMESTAMP_TRUNC`` granularity and model ``DATA_FREQUENCY``.
    id_col : str, optional
        Column used as series identifier (e.g. ``'geohash9'``,
        ``'VesselTypeName'`` or ``'MMSI'``).
    horizon : int, optional
        Forecast horizon for the ARIMA model.  A value of 1 is typical
        when the model is used solely for anomaly detection.

    Returns
    -------
    str
        A SQL string that trains (creates or replaces) the anomaly model.
    """
    table = get_table_name()
    date_filter = build_date_filter(start_date, end_date)
    vessel_filter = build_vessel_filter(vessel_types)
    freq_upper = freq.upper()
    if freq_upper == "HOURLY":
        ts_expr = "TIMESTAMP_TRUNC(BaseDateTime, HOUR)"
    elif freq_upper == "DAILY":
        ts_expr = "TIMESTAMP_TRUNC(BaseDateTime, DAY)"
    else:
        raise ValueError("freq must be 'HOURLY' or 'DAILY'")
    # Determine metric expression and label for the aggregated value
    metric_lower = metric.lower()
    if metric_lower == "count":
        metric_expr = "COUNT(*)"
    elif metric_lower == "speed":
        # Use speed over ground in knots.  Cast to FLOAT64 for accuracy.
        metric_expr = "AVG(CAST(SOG AS FLOAT64))"
    else:
        raise ValueError("metric must be 'count' or 'speed'")
    model_name = _get_model_name(metric_lower, id_col, freq_upper)
    return f"""
    CREATE OR REPLACE MODEL `{model_name}`
    OPTIONS (
      MODEL_TYPE = 'ARIMA_PLUS',
      TIME_SERIES_TIMESTAMP_COL = 'ts_col',
      TIME_SERIES_DATA_COL = 'value',
      TIME_SERIES_ID_COL = 'series_id',
      DATA_FREQUENCY = '{freq_upper}',
      HORIZON = {horizon}
    ) AS
    SELECT
      {id_col} AS series_id,
      {ts_expr} AS ts_col,
      {metric_expr} AS value
    FROM `{table}`
    WHERE {ts_expr} IS NOT NULL
      {date_filter}
      {vessel_filter}
    GROUP BY series_id, ts_col
    """


def anomaly_predict_query(
    start_date,
    end_date,
    vessel_types,
    metric,
    freq,
    id_col,
    threshold,
) -> str:
    """Build a query to detect anomalies using a trained model.

    The query constructs an evaluation dataset with the same schema as
    the training data and invokes ``ML.DETECT_ANOMALIES`` with a
    user‑defined probability threshold.  It returns all fields from
    ``ML.DETECT_ANOMALIES`` including ``is_anomaly`` and
    ``anomaly_probability``.

    Parameters
    ----------
    start_date, end_date : str
        Inclusive date range for the evaluation dataset.
    vessel_types : list[str] | None
        Optional list of vessel types to filter on.
    metric : str
        Metric modelled in the training data (``'count'`` or ``'speed'``).
    freq : str, optional
        Data frequency (``'HOURLY'`` or ``'DAILY'``).  Must match the
        frequency used during training.
    id_col : str, optional
        Series identifier column used during training.
    threshold : float, optional
        Probability threshold to flag anomalies.  Values closer to 1.0
        reduce false positives.

    Returns
    -------
    str
        A SQL string that detects anomalies for the specified period.
    """
    table = get_table_name()
    date_filter = build_date_filter(start_date, end_date)
    vessel_filter = build_vessel_filter(vessel_types)
    freq_upper = freq.upper()
    if freq_upper == "HOURLY":
        ts_expr = "TIMESTAMP_TRUNC(BaseDateTime, HOUR)"
    elif freq_upper == "DAILY":
        ts_expr = "TIMESTAMP_TRUNC(BaseDateTime, DAY)"
    else:
        raise ValueError("freq must be 'HOURLY' or 'DAILY'")
    metric_lower = metric.lower()
    if metric_lower == "count":
        metric_expr = "COUNT(*)"
    elif metric_lower == "speed":
        metric_expr = "AVG(CAST(SOG AS FLOAT64))"
    else:
        raise ValueError("metric must be 'count' or 'speed'")
    model_name = _get_model_name(metric_lower, id_col, freq_upper)
    return f"""
    SELECT
      series_id,
      ts_col,
      value,
      is_anomaly,
      anomaly_probability,
      lower_bound,
      upper_bound
    FROM ML.DETECT_ANOMALIES(
      MODEL `{model_name}`,
      STRUCT({threshold} AS anomaly_prob_threshold),
      (
        SELECT
          {id_col} AS series_id,
          {ts_expr} AS ts_col,
          {metric_expr} AS value
        FROM `{table}`
        WHERE {ts_expr} IS NOT NULL
          {date_filter}
          {vessel_filter}
        GROUP BY series_id, ts_col
      )
    )
    ORDER BY anomaly_probability DESC
    """