-- Correlación de SOG vs Draft por tipo de buque
-- Parámetros: @start_date DATE, @end_date DATE, @vessel_types ARRAY<STRING>, @min_n INT64
SELECT
VesselTypeName,
CORR(SOG, Draft) AS corr_pearson,
COUNT(*) AS n
FROM `{{TABLE}}`
WHERE SOG IS NOT NULL AND Draft IS NOT NULL
AND (@start_date IS NULL OR DATE(BaseDateTime) >= @start_date)
AND (@end_date IS NULL OR DATE(BaseDateTime) <= @end_date)
AND (ARRAY_LENGTH(@vessel_types) = 0 OR VesselTypeName IN UNNEST(@vessel_types))
GROUP BY VesselTypeName
HAVING n >= @min_n
ORDER BY corr_pearson;