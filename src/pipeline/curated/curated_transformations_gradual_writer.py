# %%
import os, sys, time, re, shlex, subprocess
from typing import List, Optional

from pyspark.sql import SparkSession, DataFrame, functions as F, types as T
from pyspark.sql.functions import pandas_udf
import pandas as pd

# %%
INPUT_BASE = "gs://bucket20250825maestria/AIS_2024_raw"
OUTPUT_BASE = "gs://bucket20250825maestria/AIS_2024_curated"

# %%
MONTHS = ["2024-08"]

# %%
SAVE_MODE = "overwrite"

# %%
TARGET_FILES_PER_PARTITION = 36

# %%
SHUFFLE_PARTITIONS = 48

# %%
RESUME_WITH_MARKERS = True
SKIP_IF_PARTITION_EXISTS = False

# %%
ARCHIVE_GCS = "gs://bucket20250825maestria/envs/pygeo-venv.tar.gz#environment"
PY_IN_ENV = "./environment/venv/bin/python"
WHEEL = "gs://bucket20250825maestria/wheels/pygeohash-1.2.0.zip"


# %%
def hadoop_path_exists(spark: SparkSession, path: str) -> bool:
    jvm = spark._jvm
    sc = spark.sparkContext
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(
        jvm.java.net.URI(path), sc._jsc.hadoopConfiguration()
    )
    return fs.exists(jvm.org.apache.hadoop.fs.Path(path))


# %%
def gsutil_prefix_exists(path: str) -> bool:
    try:
        p = subprocess.run(
            f"gsutil ls -d {shlex.quote(path)}",
            shell=True,
            capture_output=True,
            text=True,
        )
        if p.returncode == 0 and p.stdout.strip():
            return True
        p2 = subprocess.run(
            f"gsutil ls {shlex.quote(path)}**",
            shell=True,
            capture_output=True,
            text=True,
        )
        return p2.returncode == 0 and bool(p2.stdout.strip())
    except Exception:
        return False


# %%
def partition_exists(
    spark: SparkSession, base_out: str, part_col: str, part_val: str
) -> bool:
    return hadoop_path_exists(spark, f"{base_out.rstrip('/')}/{part_col}={part_val}")


# %%
def marker_path(base_out: str, part_col: str, part_val: str) -> str:
    return f"{base_out.rstrip('/')}/_markers/{part_col}={part_val}/_SUCCESS"


# %%
def marker_exists(
    spark: SparkSession, base_out: str, part_col: str, part_val: str
) -> bool:
    return hadoop_path_exists(spark, marker_path(base_out, part_col, part_val))


# %%
def save_marker(
    spark: SparkSession, base_out: str, part_col: str, part_val: str
) -> None:
    spark.sparkContext.parallelize(["ok"], 1).saveAsTextFile(
        marker_path(base_out, part_col, part_val)
    )


# %%
def month_input_path(month: str) -> str:
    return f"{INPUT_BASE.rstrip('/')}/ym={month}/"


# %%
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"

# %%
prev = SparkSession.getActiveSession()
if prev is not None:
    try:
        prev.stop()
    except Exception as e:
        print("WARN cerrando sesión previa:", e)
    time.sleep(1)

# %%
spark = (
    SparkSession.builder.appName("curated-writer-gradual")
    .master("yarn")
    .config("spark.submit.deployMode", "client")
    .config("spark.yarn.unmanagedAM.enabled", "false")
    .config("spark.yarn.dist.archives", ARCHIVE_GCS)
    .config("spark.executorEnv.PYSPARK_PYTHON", PY_IN_ENV)
    .config("spark.yarn.appMasterEnv.PYSPARK_PYTHON", PY_IN_ENV)
    .config("spark.network.timeout", "800s")
    .config("spark.shuffle.io.maxRetries", "10")
    .config("spark.shuffle.io.retryWait", "10s")
    .config("spark.reducer.maxReqsInFlight", "1")
    .config("spark.stage.maxConsecutiveAttempts", "10")
    .config("spark.shuffle.service.enabled", "true")
    .config("spark.scheduler.excludeOnFailure", "true")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS))
    .config("spark.speculation", "true")
    .getOrCreate()
)

# %%
sc = spark.sparkContext
spark.conf.set("spark.sql.session.timeZone", "UTC")
sc._jsc.hadoopConfiguration().set(
    "mapreduce.fileoutputcommitter.marksuccessfuljobs", "false"
)

# %%
spark.sparkContext.addPyFile(WHEEL)

# %%
print("AppId:", sc.applicationId)


# %%
def _make_geohash_pudf(precision: int):
    @pandas_udf("string")
    def _encode(lat: pd.Series, lon: pd.Series) -> pd.Series:
        import pygeohash as pgh

        return pd.Series(
            [
                (
                    pgh.encode(la, lo, precision=precision)
                    if pd.notnull(la) and pd.notnull(lo)
                    else None
                )
                for la, lo in zip(lat, lon)
            ]
        )

    _encode.__name__ = f"geohash_p{precision}"
    return _encode


# %%
def apply_curated_transformations(df: DataFrame) -> DataFrame:
    t0 = time.time()

    def _log(msg: str, sdf: DataFrame | None = None):
        print(f"[curated] {msg}")
        if sdf is not None:
            try:
                print(f"[curated] columnas={len(sdf.columns)}")
            except Exception as e:
                print(f"[curated] (warn) no se pudo imprimir schema: {e}")

    _log("inicio pipeline curated", df)

    df1 = (
        df.withColumn(
            "MMSI", F.regexp_extract(F.col("MMSI").cast("string"), r"(\d{1,9})$", 1)
        )
        .withColumn("MMSI", F.lpad("MMSI", 9, "0"))
        .withColumn(
            "BaseDateTime",
            F.to_timestamp(F.col("BaseDateTime"), "yyyy-MM-dd'T'HH:mm:ss"),
        )
        .withColumn("LAT", F.col("LAT").cast("double"))
        .withColumn("LON", F.col("LON").cast("double"))
        .withColumn("SOG", F.col("SOG").cast("double"))
        .withColumn("COG", F.col("COG").cast("double"))
        .withColumn("Heading", F.col("Heading").cast("double"))
        .withColumn("Length", F.col("Length").cast("double"))
        .withColumn("Width", F.col("Width").cast("double"))
        .withColumn("Draft", F.col("Draft").cast("double"))
        .withColumn("VesselName", F.trim(F.col("VesselName")))
        .withColumn("IMO", F.trim(F.col("IMO")))
        .withColumn("CallSign", F.trim(F.col("CallSign")))
        .withColumn("VesselType", F.trim(F.col("VesselType")))
        .withColumn("Cargo", F.trim(F.col("Cargo")))
        .withColumn("TransceiverClass", F.upper(F.trim(F.col("TransceiverClass"))))
    )
    _log("df1: casts y normalizaciones base aplicadas", df1)

    wrap_lon = (
        F.when(F.col("LON") > 180, F.col("LON") - 360)
        .when(F.col("LON") < -180, F.col("LON") + 360)
        .otherwise(F.col("LON"))
    )
    df2 = (
        df1.withColumn(
            "LAT",
            F.when((F.col("LAT") >= -90) & (F.col("LAT") <= 90), F.round("LAT", 5)),
        )
        .withColumn("LON", F.round(wrap_lon, 5))
        .filter(F.col("LAT").isNotNull() & F.col("LON").isNotNull())
    )
    _log("df2: coordenadas corregidas/recortadas y nulos filtrados", df2)

    df3 = (
        df2.withColumn(
            "Heading", F.when(F.col("Heading") == 511, None).otherwise(F.col("Heading"))
        )
        .withColumn(
            "Heading",
            F.when(F.col("Heading").isNotNull(), F.col("Heading") % 360).otherwise(
                None
            ),
        )
        .withColumn(
            "COG", F.when(F.col("COG").isNotNull(), F.col("COG") % 360).otherwise(None)
        )
        .withColumn(
            "SOG",
            F.when((F.col("SOG") >= 0) & (F.col("SOG") <= 70), F.col("SOG")).otherwise(
                None
            ),
        )
        .withColumn(
            "IMO",
            F.when(F.col("IMO").isin("IMO0000000", "0", ""), None).otherwise(
                F.col("IMO")
            ),
        )
        .withColumn(
            "Length",
            F.when((F.col("Length") >= 1) & (F.col("Length") <= 450), F.col("Length")),
        )
        .withColumn(
            "Width",
            F.when((F.col("Width") >= 1) & (F.col("Width") <= 70), F.col("Width")),
        )
        .withColumn(
            "Draft",
            F.when((F.col("Draft") >= 0) & (F.col("Draft") <= 25), F.col("Draft")),
        )
    )
    _log("df3: reglas de rango y normalizaciones aplicadas", df3)

    df4 = df3.withColumn(
        "VesselTypeInt",
        F.when(
            F.col("VesselType").rlike(r"^\d+$"), F.col("VesselType").cast("int")
        ).otherwise(None),
    ).withColumn("VesselTypeCode", F.col("VesselType"))

    type_map = {
        0: "Not available (default)",
        20: "Wing in ground (WIG), all ships of this type",
        21: "Wing in ground (WIG), Hazardous category A",
        22: "Wing in ground (WIG), Hazardous category B",
        23: "Wing in ground (WIG), Hazardous category C",
        24: "Wing in ground (WIG), Hazardous category D",
        25: "Wing in ground (WIG), Reserved for future use",
        26: "Wing in ground (WIG), Reserved for future use",
        27: "Wing in ground (WIG), Reserved for future use",
        28: "Wing in ground (WIG), Reserved for future use",
        29: "Wing in ground (WIG), Reserved for future use",
        30: "Fishing",
        31: "Towing",
        32: "Towing: length exceeds 200m or breadth exceeds 25m",
        33: "Dredging or underwater ops",
        34: "Diving ops",
        35: "Military ops",
        36: "Sailing",
        37: "Pleasure Craft",
        38: "Reserved",
        39: "Reserved",
        40: "High speed craft (HSC), all ships of this type",
        41: "High speed craft (HSC), Hazardous category A",
        42: "High speed craft (HSC), Hazardous category B",
        43: "High speed craft (HSC), Hazardous category C",
        44: "High speed craft (HSC), Hazardous category D",
        45: "High speed craft (HSC), Reserved for future use",
        46: "High speed craft (HSC), Reserved for future use",
        47: "High speed craft (HSC), Reserved for future use",
        48: "High speed craft (HSC), Reserved for future use",
        49: "High speed craft (HSC), No additional information",
        50: "Pilot Vessel",
        51: "Search and Rescue vessel",
        52: "Tug",
        53: "Port Tender",
        54: "Anti-pollution equipment",
        55: "Law Enforcement",
        56: "Spare - Local Vessel",
        57: "Spare - Local Vessel",
        58: "Medical Transport",
        59: "Noncombatant ship according to RR Resolution No. 18",
        60: "Passenger, all ships of this type",
        61: "Passenger, Hazardous category A",
        62: "Passenger, Hazardous category B",
        63: "Passenger, Hazardous category C",
        64: "Passenger, Hazardous category D",
        65: "Passenger, Reserved for future use",
        66: "Passenger, Reserved for future use",
        67: "Passenger, Reserved for future use",
        68: "Passenger, Reserved for future use",
        69: "Passenger, No additional information",
        70: "Cargo, all ships of this type",
        71: "Cargo, Hazardous category A",
        72: "Cargo, Hazardous category B",
        73: "Cargo, Hazardous category C",
        74: "Cargo, Hazardous category D",
        75: "Cargo, Reserved for future use",
        76: "Cargo, Reserved for future use",
        77: "Cargo, Reserved for future use",
        78: "Cargo, Reserved for future use",
        79: "Cargo, No additional information",
        80: "Tanker, all ships of this type",
        81: "Tanker, Hazardous category A",
        82: "Tanker, Hazardous category B",
        83: "Tanker, Hazardous category C",
        84: "Tanker, Hazardous category D",
        85: "Tanker, Reserved for future use",
        86: "Tanker, Reserved for future use",
        87: "Tanker, Reserved for future use",
        88: "Tanker, Reserved for future use",
        89: "Tanker, No additional information",
        90: "Other Type, all ships of this type",
        91: "Other Type, Hazardous category A",
        92: "Other Type, Hazardous category B",
        93: "Other Type, Hazardous category C",
        94: "Other Type, Hazardous category D",
        95: "Other Type, Reserved for future use",
        96: "Other Type, Reserved for future use",
        97: "Other Type, Reserved for future use",
        98: "Other Type, Reserved for future use",
        99: "Other Type, no additional information",
    }
    mapping_expr = F.create_map(
        *[x for kv in type_map.items() for x in (F.lit(int(kv[0])), F.lit(kv[1]))]
    )

    cls = (
        F.when(F.col("VesselTypeInt").between(20, 29), "WIG")
        .when(F.col("VesselTypeInt").between(30, 39), "Small/Leisure")
        .when(F.col("VesselTypeInt").between(40, 49), "HSC")
        .when(F.col("VesselTypeInt").between(50, 59), "Service/Special")
        .when(F.col("VesselTypeInt").between(60, 69), "Passenger")
        .when(F.col("VesselTypeInt").between(70, 79), "Cargo")
        .when(F.col("VesselTypeInt").between(80, 89), "Tanker")
        .when(F.col("VesselTypeInt").between(90, 99), "Other")
        .otherwise("Unspecified")
    )

    df5 = df4.withColumn(
        "VesselTypeName", mapping_expr[F.col("VesselTypeInt")]
    ).withColumn("VesselTypeClass", cls)
    _log("df5: enriquecimiento de VesselType (Int/Name/Class)", df5)

    status_map = [
        (0, "Under way using engine"),
        (1, "At anchor"),
        (2, "Not under command"),
        (3, "Restricted manoeuverability"),
        (4, "Constrained by her draught"),
        (5, "Moored"),
        (6, "Aground"),
        (7, "Engaged in fishing"),
        (8, "Under way sailing"),
        (14, "AIS-SART/MOB/EPIRB active"),
        (15, "Not defined (default)"),
    ]
    status_df = df.sparkSession.createDataFrame(
        status_map, "NavStatusInt INT, NavStatusName STRING"
    )
    df6 = (
        df5.withColumn("NavStatusInt", F.col("Status").cast("int"))
        .join(F.broadcast(status_df), on="NavStatusInt", how="left")
        .withColumn(
            "NavStatusName",
            F.when(F.col("NavStatusInt").isNull(), "Not reported")
            .when(~F.col("NavStatusInt").between(0, 15), "Unknown code")
            .otherwise(F.col("NavStatusName")),
        )
    )
    _log("df6: join catálogo estatus navegación", df6)

    def _normalize(cname: str):
        return F.when(
            F.col(cname).isNotNull(),
            F.regexp_replace(
                F.regexp_replace(F.upper(F.trim(F.col(cname))), r"[^A-Z0-9 ]", ""),
                r"\s+",
                " ",
            ),
        )

    df7 = df6.withColumn(
        "VesselName", F.coalesce(_normalize("VesselName"), F.col("VesselName"))
    ).withColumn("CallSign", F.coalesce(_normalize("CallSign"), F.col("CallSign")))
    _log("df7: normalización nombres/callsign", df7)

    df8 = (
        df7.withColumn("ym", F.date_format("BaseDateTime", "yyyy-MM"))
        .withColumn("date", F.to_date("BaseDateTime"))
        .withColumn("hour", F.hour("BaseDateTime"))
        .withColumn("dow", F.date_format("BaseDateTime", "E"))
        .withColumn("week", F.weekofyear("BaseDateTime"))
        .withColumn("month", F.month("BaseDateTime"))
        .withColumn("quarter", F.quarter("BaseDateTime"))
        .withColumn("SOG_ms", F.col("SOG") * 0.514444)
    )
    _log("df8: derivadas temporales", df8)

    gh9 = _make_geohash_pudf(9)
    df9 = df8.withColumn("geohash9", gh9(F.col("LAT"), F.col("LON")))
    _log("df9: geohash9", df9)

    df10 = df9.dropDuplicates(["MMSI", "BaseDateTime"])
    _log("df10: deduplicación final", df10)

    print(f"[curated] fin (elapsed={time.time()-t0:0.2f}s)")
    return df10


# %%
processed, skipped, failed = [], [], []

# %%
if SAVE_MODE == "overwrite":
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# %%
for m in sorted(MONTHS):
    in_path = month_input_path(m)
    exists = gsutil_prefix_exists(in_path)
    if not exists:
        print(f"[SKIP] No existe entrada {in_path}")
        skipped.append(m)
        continue

    if RESUME_WITH_MARKERS and marker_exists(spark, OUTPUT_BASE, "ym", m):
        print(f"[SKIP/MARKER] ym={m} ya tiene marker.")
        skipped.append(m)
        continue
    if SKIP_IF_PARTITION_EXISTS and partition_exists(spark, OUTPUT_BASE, "ym", m):
        print(f"[SKIP/EXISTS] ym={m} ya existe en salida.")
        skipped.append(m)
        continue

    try:
        print(f"\n=== ym={m} ===")
        print(f"[READ] {in_path}")
        df = spark.read.parquet(in_path)

        print("[XFORM] apply_curated_transformations...")
        df_t = apply_curated_transformations(df)
        if "ym" not in df_t.columns:
            df_t = df_t.withColumn("ym", F.lit(m))

        df_part = df_t.repartition(max(32, SHUFFLE_PARTITIONS // 2)).coalesce(
            TARGET_FILES_PER_PARTITION
        )

        print(f"[WRITE] {OUTPUT_BASE}  (ym={m}, mode={SAVE_MODE})")
        (
            df_part.write.mode(SAVE_MODE)
            .option("compression", "snappy")
            .partitionBy("ym")
            .parquet(OUTPUT_BASE)
        )

        save_marker(spark, OUTPUT_BASE, "ym", m)

        processed.append(m)
        print(f"[OK] ym={m} listo.")
        del df, df_t, df_part
        spark.catalog.clearCache()
    except Exception as e:
        failed.append((m, str(e)))
        print(f"[FAIL] ym={m} -> {e}")

# %%
print("\n========== RESUMEN ==========")
print(f"Procesadas: {processed}")
print(f"Saltadas:   {skipped}")

# %%
if failed:
    print("Fallidas:")
    for m, err in failed:
        print(f"  - ym={m}: {err}")
else:
    print("Fallidas:   []")

# %%
spark.stop()

# %%
