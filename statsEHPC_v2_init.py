import sys
import os
import argparse
import calendar
from pathlib import Path
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from collections import OrderedDict

import findspark
findspark.init()

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

# Directories 
CWD     = os.getcwd()
#DATADIR = '/projects/F202500010HPCVLABUMINHO/DataSets/Reports'
DATADIR = f"{CWD}/spark/datadir"
OUTDIR  = f"{CWD}/spark/outdir"
SPARK_EVENT_LOG_DIR = f"{CWD}/spark/sparkevents/local"


for p in [DATADIR, OUTDIR, SPARK_EVENT_LOG_DIR]:
    os.makedirs(p, exist_ok=True)

SPARK_UI_ACTIVE_MODE: bool = False

# Args
parser = argparse.ArgumentParser()
parser.add_argument("-m", "--month",   nargs="?", help="month abbr e.g. Mar")
parser.add_argument("-y", "--year",    nargs="?", help="year e.g. 2025")
parser.add_argument("-s", "--start",   nargs="?", help="start YYYY-MM-DD")
parser.add_argument("-o", "--outfile", nargs="?", help="output filename")
args, _ = parser.parse_known_args()

# Logic of reading the args to filter then the desiged files will be open and processed.
MONTHS_FULL = list(calendar.month_name)[1:]
MONTHS_ABR  = list(calendar.month_abbr)[1:]

today     = datetime.now().date()
year      = today.year
month_int = today.month - 2
month     = MONTHS_ABR[month_int]
syear     = date(year, 1, 1)

if args.month is not None:
    month_int = MONTHS_ABR.index(args.month)
    month     = MONTHS_ABR[month_int]
if args.year is not None:
    year  = int(args.year)
    syear = date(year, 1, 1)
if args.start is not None:
    syear = datetime.strptime(args.start, "%Y-%m-%d").date()

smonth  = date(year, month_int + 1, 1)
emonthd = smonth + relativedelta(months=1, days=-1)
emonth  = smonth + relativedelta(months=1)
tmonth  = (emonth - relativedelta(months=month_int + 1)
           if month_int < 3
           else emonth - relativedelta(months=3))

# Logic inside a dict to make this more redable.
tag_month = {
    "":          [month],
    "Trimester": (MONTHS_ABR[:month_int + 1]
                  if month_int < 3
                  else MONTHS_ABR[month_int - 2 : month_int + 1]),
    "Year":      MONTHS_ABR[:month_int + 1],
}

outfilename = args.outfile or "params.tex"
outfile_path = Path(f"{OUTDIR}/{outfilename}")
outfile_path.parent.mkdir(parents=True, exist_ok=True)

# Pre compute date values
ndays      = (emonth - smonth).days
ndays_tri  = (emonth - tmonth).days
ndays_year = (emonth - syear).days

report_period     = f"{smonth.strftime('%d/%m/%Y')} - {emonthd.strftime('%d/%m/%Y')}"
report_period_tri = (f"{syear.strftime('%d/%m/%Y')} - {emonthd.strftime('%d/%m/%Y')}"
                     if month_int < 3
                     else f"{tmonth.strftime('%d/%m/%Y')} - {emonthd.strftime('%d/%m/%Y')}")
report_period_yr  = f"{syear.strftime('%d/%m/%Y')} - {emonthd.strftime('%d/%m/%Y')}"

# This way it can be mapped in the SQL with the right types and values.
# Not all of them are here because some will be create by the SQL query with the right values.
PARAM_ORDER = OrderedDict([
    # ── Report dates ──────────────────────────────────────
    ("reportPeriod",              ("STR",   report_period)),
    ("reportPeriodTrimester",     ("STR",   report_period_tri)),
    ("reportPeriodYear",          ("STR",   report_period_yr)),
    ("reportMonth",               ("STR",   MONTHS_FULL[month_int])),
    ("reportYear",                ("INT",   str(year))),
    # ── Machine config ────────────────────────────────────
    ("armnodes",                  ("INT",   "1632")),
    ("amdnodes",                  ("INT",   "500")),
    ("gpunodes",                  ("INT",   "132")),
    ("percentaviail",             ("DBL",   "0.8")),
    ("eurohpcavail",              ("DBL",   "0.35")),
    # ── Days ──────────────────────────────────────────────
    ("ndays",                     ("INT",   str(ndays))),
    # ── Monthly: hours (computed) ─────────────────────────
    ("armusedhours",              ("DBL",   None)),
    ("amdusedhours",              ("DBL",   None)),
    ("gpuusedhours",              ("DBL",   None)),
    # ── Monthly: EuroHPC hours (computed) ─────────────────
    ("gpuusedhoursEuroHPC",       ("DBL",   None)),
    ("amdusedhoursEuroHPC",       ("DBL",   None)),
    ("armusedhoursEuroHPC",       ("DBL",   None)),
    # ── Monthly: jobs (computed) ──────────────────────────
    ("armJobs",                   ("INT",   None)),
    ("amdJobs",                   ("INT",   None)),
    ("gpuJobs",                   ("INT",   None)),
    # ── Monthly: completed/failed (computed) ──────────────
    ("gpuCompletedJobs",          ("INT",   None)),
    ("gpuFailedJobs",             ("INT",   None)),
    ("armCompletedJobs",          ("INT",   None)),
    ("amdCompletedJobs",          ("INT",   None)),
    ("amdFailedJobs",             ("INT",   None)),
    ("armFailedJobs",             ("INT",   None)),
    # ── Monthly: EuroHPC jobs (computed) ──────────────────
    ("gpuJobsEuroHPC",            ("INT",   None)),
    ("amdJobsEuroHPC",            ("INT",   None)),
    ("armJobsEuroHPC",            ("INT",   None)),
    # ── Trimester days ────────────────────────────────────
    ("ndaysTrimester",            ("INT",   str(ndays_tri))),
    # ── Trimester: completed/failed (computed) ────────────
    ("gpuCompletedJobsTrimester", ("INT",   None)),
    ("gpuFailedJobsTrimester",    ("INT",   None)),
    ("armCompletedJobsTrimester", ("INT",   None)),
    ("amdCompletedJobsTrimester", ("INT",   None)),
    ("amdFailedJobsTrimester",    ("INT",   None)),
    ("armFailedJobsTrimester",    ("INT",   None)),
    # ── Trimester: hours (computed) ───────────────────────
    ("armusedhoursTrimester",     ("DBL",   None)),
    ("amdusedhoursTrimester",     ("DBL",   None)),
    ("gpuusedhoursTrimester",     ("DBL",   None)),
    # ── Trimester: jobs (computed) ────────────────────────
    ("armJobsTrimester",          ("INT",   None)),
    ("amdJobsTrimester",          ("INT",   None)),
    ("gpuJobsTrimester",          ("INT",   None)),
    # ── Trimester: EuroHPC hours (computed) ───────────────
    ("gpuusedhoursEuroHPCTrimester", ("DBL", None)),
    ("amdusedhoursEuroHPCTrimester", ("DBL", None)),
    ("armusedhoursEuroHPCTrimester", ("DBL", None)),
    # ── Trimester: EuroHPC jobs (computed) ────────────────
    ("gpuJobsEuroHPCTrimester",   ("INT",   None)),
    ("amdJobsEuroHPCTrimester",   ("INT",   None)),
    ("armJobsEuroHPCTrimester",   ("INT",   None)),
    # ── Year days ─────────────────────────────────────────
    ("ndaysYear",                 ("INT",   str(ndays_year))),
    # ── Year: completed/failed (computed) ─────────────────
    ("gpuCompletedJobsYear",      ("INT",   None)),
    ("gpuFailedJobsYear",         ("INT",   None)),
    ("armCompletedJobsYear",      ("INT",   None)),
    ("amdCompletedJobsYear",      ("INT",   None)),
    ("amdFailedJobsYear",         ("INT",   None)),
    ("armFailedJobsYear",         ("INT",   None)),
    # ── Year: hours (computed) ────────────────────────────
    ("armusedhoursYear",          ("DBL",   None)),
    ("amdusedhoursYear",          ("DBL",   None)),
    ("gpuusedhoursYear",          ("DBL",   None)),
    # ── Year: jobs (computed) ─────────────────────────────
    ("armJobsYear",               ("INT",   None)),
    ("amdJobsYear",               ("INT",   None)),
    ("gpuJobsYear",               ("INT",   None)),
    # ── Year: EuroHPC jobs (computed) ─────────────────────
    ("gpuJobsEuroHPCYear",        ("INT",   None)),
    ("amdJobsEuroHPCYear",        ("INT",   None)),
    ("armJobsEuroHPCYear",        ("INT",   None)),
    # ── Year: EuroHPC hours (computed) ────────────────────
    ("gpuusedhoursEuroHPCYear",   ("DBL",   None)),
    ("amdusedhoursEuroHPCYear",   ("DBL",   None)),
    ("armusedhoursEuroHPCYear",   ("DBL",   None)),
    # ── LaTeX formulas ────────────────────────────────────
    ("monthhours",                ("LATEX", r"{\inteval{\ndays * 24}}")),
    ("hoursTrimester",            ("LATEX", r"{\inteval{\ndaysTrimester * 24}}")),
    ("hoursYear",                 ("LATEX", r"{\inteval{\ndaysYear * 24}}")),
])

# Instead of hardcoded now this permutes and makes all the right names and tags
METRIC_MAP = {}
for key, (dtype, default) in PARAM_ORDER.items():
    if default is not None:
        continue  # static value, no need to compute

    # Parse the key to find cluster, metric name, window tag
    k = key
    tag = ""
    for t in ["Trimester", "Year"]:
        if k.endswith(t):
            tag = t
            k = k[: -len(t)]
            break

    cluster = None
    metric  = None
    for cl in ["gpu", "amd", "arm"]:
        if k.startswith(cl):
            cluster = cl.upper()
            remainder = k[len(cl):]

            if remainder == "usedhours":
                metric = "used_hours"
            elif remainder == "usedhoursEuroHPC":
                metric = "eurohpc_hours"
            elif remainder == "Jobs":
                metric = "jobs_no_local"
            elif remainder == "CompletedJobs":
                metric = "completed_jobs"
            elif remainder == "FailedJobs":
                metric = "failed_jobs"
            elif remainder == "JobsEuroHPC":
                metric = "eurohpc_jobs"
            break

    if cluster and metric:
        METRIC_MAP[key] = (cluster, metric, tag)

# File search
all_months_needed = set()
for ms in tag_month.values():
    all_months_needed.update(ms)

walk_start = min(syear, tmonth, smonth)
cur = date(walk_start.year, walk_start.month, 1)
years_months: dict[int, list[str]] = {}
while cur <= emonthd:
    m_abr = cur.strftime("%b")
    if m_abr in all_months_needed:
        years_months.setdefault(cur.year, []).append(m_abr)
    cur += relativedelta(months=1)

csv_paths: list[str] = []
for yr, mlist in years_months.items():
    year_dir = f"{DATADIR}/{yr}"
    if not os.path.isdir(year_dir):
        continue
    for f in os.listdir(year_dir):
        month_file = "_".join(f.split("_")[1:]).split(".")[0]
        if month_file in mlist:
            csv_paths.append(f"{year_dir}/{f}")
            print(f"  Queued: {f}  (month={month_file})")

if not csv_paths:
    sys.exit("ERROR: No data files found.")


spark = (
    SparkSession.builder
    .master("local[4]")
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", SPARK_EVENT_LOG_DIR)
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

window_rows = [(tag, m) for tag, ms in tag_month.items() for m in ms]
spark.createDataFrame(window_rows, schema=["window_tag", "period_month"]) \
     .createOrReplaceTempView("time_windows")

# Struct that match the CSV
schema = T.StructType([
    T.StructField("JobID",         T.StringType(),  True),
    T.StructField("JobIDRaw",      T.StringType(),  True),
    T.StructField("ElapsedRaw",    T.IntegerType(), True),
    T.StructField("Account",       T.StringType(),  True),
    T.StructField("AllocCPUS",     T.IntegerType(), True),
    T.StructField("CPUTimeRAW",    T.LongType(),    True),
    T.StructField("NNodes",        T.IntegerType(), True),
    T.StructField("AllocNodes",    T.IntegerType(), True),
    T.StructField("NCPUS",         T.IntegerType(), True),
    T.StructField("Partition",     T.StringType(),  True),
    T.StructField("TotalCPU",      T.StringType(),  True),
    T.StructField("User",          T.StringType(),  True),
    T.StructField("ExitCode",      T.StringType(),  True),
    T.StructField("State",         T.StringType(),  True),
    T.StructField("Reservation",   T.StringType(),  True),
    T.StructField("ReservationId", T.StringType(),  True),
    T.StructField("AllocTRES",     T.StringType(),  True),
])

(spark.read
     .option("sep", "|")
     .csv(csv_paths, schema=schema, header=True)
     .select("ElapsedRaw", "Account", "AllocCPUS", "NNodes",
             "Partition", "State", "AllocTRES")
     .withColumn("Period", F.lit(month))
     .createOrReplaceTempView("raw_data"))

print("raw_data loaded")


spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW enriched AS
    SELECT *,
           (ElapsedRaw * VNodes) AS totalJobSeconds
    FROM (
        SELECT
            ElapsedRaw, Account, AllocCPUS, NNodes,
            Partition, State, AllocTRES, Period,

            regexp_replace(State, 'CANCELLED(.*)', 'CANCELLED') AS EState,

            CASE WHEN State = 'COMPLETED' THEN 'COMPLETED'
                 ELSE 'FAILED'
            END AS COMPLETED,

            CASE WHEN Partition LIKE '%arm%'  THEN 'ARM'
                 WHEN Partition LIKE '%a100%' THEN 'GPU'
                 ELSE 'AMD'
            END AS cluster,

            CASE WHEN Account LIKE 'f%'  THEN 'FCT'
                 WHEN Account LIKE 'ee%' THEN 'EHPC'
                 ELSE 'LOCAL'
            END AS Agency,

            CASE WHEN Partition LIKE '%a100%'
                 THEN CASE
                          WHEN AllocTRES IS NULL THEN NNodes
                          WHEN AllocTRES RLIKE 'gres/gpu=([0-9]+)'
                               THEN CAST(regexp_extract(AllocTRES,
                                         'gres/gpu=([0-9]+)', 1) AS INT)
                          ELSE NNodes * 4
                      END
                 ELSE NNodes
            END AS VNodes
        FROM raw_data
    )
""")
spark.catalog.cacheTable("enriched")
row_count = spark.sql("SELECT COUNT(*) AS n FROM enriched").collect()[0].n
print(f"enriched cached ({row_count} rows)")


spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW metrics AS
    SELECT
        w.window_tag,
        e.cluster,

        SUM(CASE WHEN e.Agency != 'LOCAL' THEN 1 ELSE 0 END)        AS jobs_no_local,
        SUM(CASE WHEN e.COMPLETED = 'COMPLETED' THEN 1 ELSE 0 END)  AS completed_jobs,
        SUM(CASE WHEN e.COMPLETED = 'FAILED'    THEN 1 ELSE 0 END)  AS failed_jobs,

        COALESCE(CAST(SUM(CASE WHEN e.Agency != 'LOCAL'
                          THEN e.totalJobSeconds END) AS DOUBLE) / 3600.0, 0.0)   AS used_hours,

        SUM(CASE WHEN e.Agency = 'EHPC' THEN 1 ELSE 0 END)          AS eurohpc_jobs,
        COALESCE(CAST(SUM(CASE WHEN e.Agency = 'EHPC'
                          THEN e.totalJobSeconds END) AS DOUBLE) / 3600.0, 0.0)   AS eurohpc_hours

    FROM enriched e
    JOIN time_windows w ON e.Period = w.period_month
    GROUP BY w.window_tag, e.cluster
""")

#spark.sql("SELECT * FROM metrics").show(50, truncate=False)
print("metrics created")

registry_rows = []   # (pos, param_key, dtype, val_element_or_None)
for pos, (key, (dtype, default)) in enumerate(PARAM_ORDER.items()):
    registry_rows.append((pos, key, dtype, default))

registry_schema = T.StructType([
    T.StructField("pos",       T.IntegerType()),
    T.StructField("param_key", T.StringType()),
    T.StructField("dtype",     T.StringType()),
    T.StructField("static_val", T.StringType()),   # NULL for computed
])
spark.createDataFrame(registry_rows, schema=registry_schema) \
     .createOrReplaceTempView("registry_skeleton")

computed_rows = []  # (param_key, cluster, metric_col, window_tag)
for key, (cluster, metric, tag) in METRIC_MAP.items():
    computed_rows.append((key, cluster, metric, tag))

computed_schema = T.StructType([
    T.StructField("param_key",  T.StringType()),
    T.StructField("cluster",    T.StringType()),
    T.StructField("metric_col", T.StringType()),
    T.StructField("window_tag", T.StringType()),
])
spark.createDataFrame(computed_rows, schema=computed_schema) \
     .createOrReplaceTempView("computed_lookup")

spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW param_registry AS
    SELECT
        r.pos,
        r.param_key,
        r.dtype,
        ARRAY(
            CASE
                -- Static value: already known
                WHEN r.static_val IS NOT NULL
                    THEN r.static_val

                -- INT metrics: cast to int then string
                WHEN cl.metric_col = 'jobs_no_local'
                    THEN CAST(CAST(m.jobs_no_local AS INT) AS STRING)
                WHEN cl.metric_col = 'completed_jobs'
                    THEN CAST(CAST(m.completed_jobs AS INT) AS STRING)
                WHEN cl.metric_col = 'failed_jobs'
                    THEN CAST(CAST(m.failed_jobs AS INT) AS STRING)
                WHEN cl.metric_col = 'eurohpc_jobs'
                    THEN CAST(CAST(m.eurohpc_jobs AS INT) AS STRING)

                -- DBL metrics: placeholder, real value in val_dbl
                WHEN cl.metric_col IN ('used_hours', 'eurohpc_hours')
                    THEN '__DBL__'

                ELSE '0'
            END
        ) AS val,

        -- Raw double for precision-sensitive values
        CASE
            WHEN cl.metric_col = 'used_hours'    THEN m.used_hours
            WHEN cl.metric_col = 'eurohpc_hours' THEN m.eurohpc_hours
            ELSE NULL
        END AS val_dbl

    FROM registry_skeleton r

    JOIN computed_lookup cl
        ON r.param_key = cl.param_key

    JOIN metrics m
        ON  cl.cluster    = m.cluster
        AND cl.window_tag = m.window_tag

    ORDER BY r.pos
""")


#spark.sql("SELECT * FROM param_registry").show(100, truncate=False)
print("param_registry built")

rows = spark.sql("""
    SELECT pos, param_key, dtype, val, val_dbl
    FROM param_registry
    ORDER BY pos
""").collect()

tex_lines = []
for row in rows:
    key = row.param_key
    if row.val_dbl is not None:
        value = str(row.val_dbl)
        print(f"  DBL  {key} = {value}  (raw: {row.val_dbl!r})")
    else:
        value = row.val[0]
    tex_lines.append(f"\\def\\{key}{{{value}}}")

with open(outfile_path, "w", encoding="utf-8") as wf:
    wf.write("\n".join(tex_lines) + "\n")

print(f"\n✓ Wrote {len(tex_lines)} LaTeX defs → {outfile_path}")


print(f"SPARK UI URL: {spark.sparkContext.uiWebUrl}")
if SPARK_UI_ACTIVE_MODE:
    input("Press Enter to exit...")

spark.catalog.uncacheTable("enriched")
spark.stop()