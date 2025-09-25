-- Incoherencias: Moored / At Anchor con SOG > umbral
-- Parámetros: @start_date DATE, @end_date DATE, @sog_thr FLOAT64, @limit INT64
SELECT
MMSI, ANY_VALUE(VesselName) AS VesselName,
BaseDateTime, NavStatusName, SOG
FROM `{{TABLE}}`
WHERE NavStatusName IN ('Moored','At Anchor')
AND SOG > @sog_thr
AND (@start_date IS NULL OR DATE(BaseDateTime) >= @start_date)
AND (@end_date IS NULL OR DATE(BaseDateTime) <= @end_date)
ORDER BY SOG DESC
LIMIT @limit;