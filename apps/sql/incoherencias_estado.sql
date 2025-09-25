-- Resumen por estado en un día (o rango)
-- Parámetros: @start_date DATE, @end_date DATE, @nav_status ARRAY<STRING>
SELECT
DATE(BaseDateTime) AS d,
NavStatusName,
AVG(Draft) AS avg_draft,
MAX(SOG) AS max_sog,
COUNT(*) AS msgs
FROM `{{TABLE}}`
WHERE (@start_date IS NULL OR DATE(BaseDateTime) >= @start_date)
AND (@end_date IS NULL OR DATE(BaseDateTime) <= @end_date)
AND (ARRAY_LENGTH(@nav_status) = 0 OR NavStatusName IN UNNEST(@nav_status))
GROUP BY d, NavStatusName
ORDER BY d, NavStatusName;