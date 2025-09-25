-- Variabilidad (desviación estándar) de SOG y Heading por tipo
-- Parámetros: @start_date DATE, @end_date DATE, @vessel_types ARRAY<STRING>, @min_n INT64
SELECT
VesselTypeName,
STDDEV_SAMP(SOG) AS sd_sog,
STDDEV_SAMP(Heading) AS sd_heading,
COUNT(*) AS n
FROM `{{TABLE}}`
WHERE (@start_date IS NULL OR DATE(BaseDateTime) >= @start_date)
AND (@end_date IS NULL OR DATE(BaseDateTime) <= @end_date)
AND (ARRAY_LENGTH(@vessel_types) = 0 OR VesselTypeName IN UNNEST(@vessel_types))
GROUP BY VesselTypeName
HAVING n >= @min_n
ORDER BY sd_sog DESC;