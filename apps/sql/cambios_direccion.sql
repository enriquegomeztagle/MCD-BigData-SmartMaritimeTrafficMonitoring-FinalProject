-- Cambios fuertes de rumbo entre mensajes consecutivos
-- Parámetros: @start_date DATE, @end_date DATE, @max_dt_min INT64, @min_delta_deg FLOAT64,
--             @mmsi ARRAY<INT64>, @lat_min FLOAT64, @lat_max FLOAT64, @lon_min FLOAT64, @lon_max FLOAT64, @limit INT64
WITH seq AS (
  SELECT
    MMSI, BaseDateTime, COG, LAT, LON,
    LAG(COG) OVER (PARTITION BY MMSI ORDER BY BaseDateTime) AS prev_cog,
    LAG(BaseDateTime) OVER (PARTITION BY MMSI ORDER BY BaseDateTime) AS prev_ts
  FROM `{{TABLE}}`
  WHERE COG IS NOT NULL
    -- Ventana de fechas: incluye todo el día end_date ( [start, end+1d) )
    AND (@start_date IS NULL OR BaseDateTime >= TIMESTAMP(@start_date))
    AND (@end_date   IS NULL OR BaseDateTime <  TIMESTAMP(DATE_ADD(@end_date, INTERVAL 1 DAY)))
)
SELECT
  MMSI, BaseDateTime, prev_ts,
  TIMESTAMP_DIFF(BaseDateTime, prev_ts, MINUTE) AS dt_min,
  prev_cog, COG,
  ABS(((COG - prev_cog + 180.0) - 360.0 * FLOOR((COG - prev_cog + 180.0) / 360.0)) - 180.0) AS delta_deg,
  LAT, LON
FROM seq
WHERE prev_cog IS NOT NULL
  AND TIMESTAMP_DIFF(BaseDateTime, prev_ts, MINUTE) <= @max_dt_min
  AND ABS(((COG - prev_cog + 180.0) - 360.0 * FLOOR((COG - prev_cog + 180.0) / 360.0)) - 180.0) >= @min_delta_deg
  AND (ARRAY_LENGTH(@mmsi) = 0 OR MMSI IN UNNEST(@mmsi))
  AND (@lat_min IS NULL OR LAT >= @lat_min)
  AND (@lat_max IS NULL OR LAT <= @lat_max)
  AND (@lon_min IS NULL OR LON >= @lon_min)
  AND (@lon_max IS NULL OR LON <= @lon_max)
ORDER BY BaseDateTime DESC
LIMIT @limit;
