-- Parámetros: @start_date DATE, @end_date DATE, @vessel_types ARRAY<STRING>, @p INT64, @limit INT64
WITH p AS (
SELECT
VesselTypeName AS vt,
APPROX_QUANTILES(SOG, 101)[OFFSET(@p)] AS sog_p
FROM `{{TABLE}}`
WHERE SOG IS NOT NULL
AND (@start_date IS NULL OR DATE(BaseDateTime) >= @start_date)
AND (@end_date IS NULL OR DATE(BaseDateTime) <= @end_date)
AND (ARRAY_LENGTH(@vessel_types) = 0 OR VesselTypeName IN UNNEST(@vessel_types))
GROUP BY vt
)
SELECT
a.VesselTypeName,
a.MMSI,
ANY_VALUE(a.VesselName) AS VesselName,
MAX(a.SOG) AS sog_max,
p.sog_p
FROM `{{TABLE}}` a
JOIN p ON a.VesselTypeName = p.vt
WHERE a.SOG > p.sog_p
GROUP BY a.VesselTypeName, a.MMSI, p.sog_p
ORDER BY sog_max DESC
LIMIT @limit;