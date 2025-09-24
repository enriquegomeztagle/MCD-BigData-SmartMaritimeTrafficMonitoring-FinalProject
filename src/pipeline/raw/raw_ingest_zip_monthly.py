# %%
import os
import re
import sys
import json
import datetime
import logging
from typing import List
from zipfile import ZipFile

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.sql.functions import (
    input_file_name,
    count,
    current_timestamp,
    to_timestamp,
    date_format,
    coalesce,
    lit,
    col,
    regexp_extract,
)

from google.cloud import storage

# %%
DEFAULT_PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")
DEFAULT_BUCKET = "bucket20250825maestria"
DEFAULT_ZIP_PREFIX = "AIS_2024"
DEFAULT_OUT_UNZIPPED_PREFIX = "tmp_unzipped/AIS_2024/"
DEFAULT_OUT_PARQUET_PREFIX = "AIS_2024_raw/"
DEFAULT_CSV_OPTS = {"header": True}
DEFAULT_CLEANUP_UNZIPPED = True

DEFAULT_ZIP_NAME_REGEX = r"^AIS_2024_10_.*\.zip$"

# %%
args = sys.argv
if len(args) >= 6:
    PROJECT_ID = args[1]
    BUCKET = args[2]
    ZIP_PREFIX = args[3]
    OUT_UNZIPPED_PREFIX = args[4].rstrip("/") + "/"
    OUT_PARQUET_PREFIX = args[5].rstrip("/") + "/"
    CSV_OPTS = json.loads(args[6]) if len(args) > 6 else DEFAULT_CSV_OPTS
    CLEANUP_UNZIPPED = (
        (str(args[7]).lower() == "true") if len(args) > 7 else DEFAULT_CLEANUP_UNZIPPED
    )
else:
    PROJECT_ID = DEFAULT_PROJECT_ID
    BUCKET = DEFAULT_BUCKET
    ZIP_PREFIX = DEFAULT_ZIP_PREFIX
    OUT_UNZIPPED_PREFIX = DEFAULT_OUT_UNZIPPED_PREFIX
    OUT_PARQUET_PREFIX = DEFAULT_OUT_PARQUET_PREFIX
    CSV_OPTS = DEFAULT_CSV_OPTS
    CLEANUP_UNZIPPED = DEFAULT_CLEANUP_UNZIPPED

# %%
ZIP_NAME_REGEX = os.getenv("ZIP_NAME_REGEX", DEFAULT_ZIP_NAME_REGEX)
zip_name_re = re.compile(ZIP_NAME_REGEX)
CSV_OPTS_NORM = {
    k: (str(v).lower() if isinstance(v, bool) else v) for k, v in CSV_OPTS.items()
}


# %%
def _is_safe_tmp_prefix(prefix: str) -> bool:
    return prefix.endswith("/") and ("tmp_unzipped" in prefix)


# %%
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ais-jan")

# %%
spark = SparkSession.builder.appName("AIS-2024-10-to-parquet").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
sc = spark.sparkContext
log.info("Spark session initialized.")

# %%
sc._jsc.hadoopConfiguration().set(
    "mapreduce.fileoutputcommitter.marksuccessfuljobs", "false"
)

# %%
gcs = storage.Client(project=PROJECT_ID)
bkt = gcs.bucket(BUCKET)

# %%
log.info(f"Listing ZIPs in gs://{BUCKET}/{ZIP_PREFIX} ...")
all_zip_blob_names: List[str] = [
    blob.name
    for blob in gcs.list_blobs(BUCKET, prefix=ZIP_PREFIX)
    if blob.name.lower().endswith(".zip")
]
zip_blob_names = [
    n for n in all_zip_blob_names if zip_name_re.search(os.path.basename(n))
]
if not zip_blob_names:
    raise SystemExit("No ZIP files matched regex")
log.info(f"ZIPs seleccionados: {zip_blob_names}")


# %%
def unzip_one(zip_name: str) -> tuple[str, int]:
    import tempfile, os
    from google.cloud import storage
    from google.api_core import exceptions as gax_exceptions
    import time

    client = storage.Client(project=PROJECT_ID)
    bkt = client.bucket(BUCKET)

    with tempfile.NamedTemporaryFile(
        prefix="zip_", suffix=".zip", delete=False
    ) as tmpf:
        tmp_path = tmpf.name

    try:
        blob = bkt.blob(zip_name)
        blob.download_to_filename(tmp_path, timeout=60)

        extracted = 0
        zip_stem = os.path.splitext(os.path.basename(zip_name))[0]
        base_prefix = f"{OUT_UNZIPPED_PREFIX}{zip_stem}/"

        with ZipFile(tmp_path, "r") as zf:
            for info in zf.infolist():
                if info.is_dir():
                    continue
                safe = info.filename.replace("\\", "/").lstrip("/")
                out_blob = bkt.blob(f"{base_prefix}{safe}")
                with zf.open(info, "r") as member_fp:
                    out_blob.upload_from_file(member_fp, rewind=True, timeout=60)
                extracted += 1
        return (zip_name, extracted)
    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass


# %%
stats = (
    sc.parallelize(zip_blob_names, numSlices=min(4, len(zip_blob_names)))
    .map(unzip_one)
    .collect()
)
files_unzipped = int(sum(n for _, n in stats))
log.info(f"Extracted {files_unzipped} CSV(s).")

# %%
schema = StructType(
    [
        StructField("MMSI", LongType(), True),
        StructField("BaseDateTime", StringType(), True),
        StructField("LAT", DoubleType(), True),
        StructField("LON", DoubleType(), True),
        StructField("SOG", DoubleType(), True),
        StructField("COG", DoubleType(), True),
        StructField("Heading", DoubleType(), True),
        StructField("VesselName", StringType(), True),
        StructField("IMO", StringType(), True),
        StructField("CallSign", StringType(), True),
        StructField("VesselType", StringType(), True),
        StructField("Status", StringType(), True),
        StructField("Length", DoubleType(), True),
        StructField("Width", DoubleType(), True),
        StructField("Draft", DoubleType(), True),
        StructField("Cargo", StringType(), True),
        StructField("TransceiverClass", StringType(), True),
    ]
)

# %%
csv_gcs_paths = []
for z in zip_blob_names:
    stem = os.path.splitext(os.path.basename(z))[0]
    for blob in gcs.list_blobs(BUCKET, prefix=f"{OUT_UNZIPPED_PREFIX}{stem}/"):
        if blob.name.lower().endswith(".csv"):
            csv_gcs_paths.append(f"gs://{BUCKET}/{blob.name}")

if not csv_gcs_paths:
    raise SystemExit("No CSVs found after unzip")

# %%
reader = spark.read.options(**CSV_OPTS_NORM)
df = reader.csv(csv_gcs_paths, schema=schema)

# %%
df = df.withColumn("_source_file", input_file_name()).withColumn(
    "_ingest_ts", current_timestamp()
)
df = df.withColumn("ym", regexp_extract("BaseDateTime", r"^(\d{4}-\d{2})", 1))
df = df.withColumn("ymd", regexp_extract("BaseDateTime", r"^(\d{4}-\d{2}-\d{2})", 1))

# %%
df = df.persist()
csv_row_count = df.count()
log.info(f"CSV row count total: {csv_row_count:,}")

# %%
out_parquet_uri = f"gs://{BUCKET}/{OUT_PARQUET_PREFIX}"
(
    df.write.mode("overwrite")
    .option("partitionOverwriteMode", "dynamic")
    .partitionBy("ym")
    .parquet(out_parquet_uri)
)
log.info(f"Parquet written to {out_parquet_uri}")

# %%
yms = [r["ym"] for r in df.select("ym").distinct().collect()]
ym_paths = [f"{out_parquet_uri}/ym={ym}" for ym in yms if ym]
parq_df = (
    spark.read.parquet(*ym_paths)
    .withColumn("ymd", date_format(to_timestamp("BaseDateTime"), "yyyy-MM-dd"))
    .persist()
)
parq_row_count = parq_df.count()
log.info(f"Parquet row count total: {parq_row_count:,}")

# %%
csv_day_counts = df.groupBy("ymd").count().withColumnRenamed("count", "csv_rows")
parq_day_counts = (
    parq_df.groupBy("ymd").count().withColumnRenamed("count", "parquet_rows")
)
day_compare = (
    csv_day_counts.join(parq_day_counts, on="ymd", how="full")
    .withColumn("csv_rows", coalesce(col("csv_rows"), lit(0)))
    .withColumn("parquet_rows", coalesce(col("parquet_rows"), lit(0)))
    .withColumn("match", col("csv_rows") == col("parquet_rows"))
    .orderBy("ymd")
)

rows = day_compare.collect()
log.info("===== Comparación de conteos por día =====")
for r in rows:
    log.info(
        f"{r['ymd']}: CSV={int(r['csv_rows']):,} | Parquet={int(r['parquet_rows']):,} | match={r['match']}"
    )

mismatches = [r for r in rows if not r["match"]]
if mismatches:
    log.error("¡Hay diferencias por día!")
    for r in mismatches:
        log.error(
            f"  {r['ymd']}: CSV={int(r['csv_rows'])}, Parquet={int(r['parquet_rows'])}"
        )
    raise SystemExit(2)
else:
    log.info("Validación por día PASSED.")

# %%
success_blob = bkt.blob(f"{OUT_PARQUET_PREFIX}_SUCCESS")
if success_blob.exists():
    success_blob.delete()
    log.info("Deleted _SUCCESS file.")

if CLEANUP_UNZIPPED and _is_safe_tmp_prefix(OUT_UNZIPPED_PREFIX):
    log.info(f"Cleaning tmp under gs://{BUCKET}/{OUT_UNZIPPED_PREFIX} ...")
    for blob in gcs.list_blobs(BUCKET, prefix=OUT_UNZIPPED_PREFIX):
        blob.delete()

# %%
spark.stop()
log.info("Job finished successfully.")

# %%
