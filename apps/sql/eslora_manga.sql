-- Correlación Eslora vs Manga por clase de buque
-- Parámetros: @start_date DATE, @end_date DATE, @classes ARRAY<STRING>, @min_n INT64
SELECT
VesselTypeClass,
CORR(Length, Width) AS corr_len_width,
COVAR_POP(Length, Width) AS covar_len_width,
COUNT(*) AS n
FROM `{{TABLE}}`
WHERE Length IS NOT NULL AND Width IS NOT NULL
AND (@start_date IS NULL OR DATE(BaseDateTime) >= @start_date)
AND (@end_date IS NULL OR DATE(BaseDateTime) <= @end_date)
AND (ARRAY_LENGTH(@classes) = 0 OR VesselTypeClass IN UNNEST(@classes))
GROUP BY VesselTypeClass
HAVING n >= @min_n
ORDER BY corr_len_width DESC;