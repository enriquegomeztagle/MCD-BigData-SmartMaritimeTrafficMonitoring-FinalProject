WITH t AS (
  SELECT
    VesselTypeName,
    NavStatusName,
    FORMAT_DATE('%A', DATE(BaseDateTime)) AS dow,
    EXTRACT(DAYOFWEEK FROM DATE(BaseDateTime)) AS dow_sun1 -- 1=Sun..7=Sat
  FROM `dataset_AIS_2024_silver_refined.table_AIS_2024_silver_refined`
  WHERE VesselTypeName IS NOT NULL AND NavStatusName IS NOT NULL
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
  (MOD(dow_sun1 + 5, 7) + 1);  -- L=1, M=2, …, D=7
