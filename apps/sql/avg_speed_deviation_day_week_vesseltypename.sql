
WITH t AS (
  SELECT
    VesselTypeName,
    FORMAT_DATE('%A', DATE(BaseDateTime)) AS dow,
    EXTRACT(DAYOFWEEK FROM DATE(BaseDateTime)) AS dow_sun1, -- 1=Dom ... 7=Sáb
    SOG
  FROM `dataset_AIS_2024_silver_refined.table_AIS_2024_silver_refined`
  WHERE VesselTypeName IS NOT NULL
)
SELECT
  VesselTypeName,
  dow,
  AVG(SOG)         AS avg_sog,
  STDDEV_SAMP(SOG) AS sd_sog,
  COUNT(*)         AS n
FROM t
GROUP BY VesselTypeName, dow, dow_sun1
ORDER BY
  VesselTypeName,
  (MOD(dow_sun1 + 5, 7) + 1);  -- rota para que L=1, M=2, …, D=7
