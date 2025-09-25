-- Calado anómalo por buque (z-score sobre promedio por tipo)
-- Parámetros: @start_date DATE, @end_date DATE, @vessel_types ARRAY<STRING>, @z_min FLOAT64, @limit INT64
WITH per_vessel AS (
SELECT
VesselTypeName AS vt,
MMSI,
AVG(CAST(Draft AS FLOAT64)) AS avg_draft
FROM `{{TABLE}}`
WHERE Draft IS NOT NULL
AND (@start_date IS NULL OR DATE(BaseDateTime) >= @start_date)
AND (@end_date IS NULL OR DATE(BaseDateTime) <= @end_date)
AND (ARRAY_LENGTH(@vessel_types) = 0 OR VesselTypeName IN UNNEST(@vessel_types))
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
AND ABS(p.avg_draft - s.mu) / s.sd >= @z_min
ORDER BY z DESC
LIMIT @limit;