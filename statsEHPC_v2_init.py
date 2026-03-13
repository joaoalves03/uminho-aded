
import sys
import subprocess
from pathlib import Path

import findspark
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import argparse
import calendar
import os
from datetime import date, timedelta, datetime, time
from dateutil.relativedelta import relativedelta



# # Code
findspark.init()

# Adding the Global variables to make interchange between the local and the cluster environment easier


CURRENT_WORK_DIRECTORY = os.getcwd()

print(f"Current working directory: {CURRENT_WORK_DIRECTORY}")

#DATADIR = '/projects/F202500010HPCVLABUMINHO/DataSets/Reports'
DATADIR = f'{CURRENT_WORK_DIRECTORY}/spark/datadir'
OUTDIR  = f'{CURRENT_WORK_DIRECTORY}/spark/outdir'
SPARK_EVENT_LOG_DIR = f'{CURRENT_WORK_DIRECTORY}/spark/sparkevents/local'

for path in [DATADIR, OUTDIR, SPARK_EVENT_LOG_DIR]:
    os.makedirs(path, exist_ok=True)
    print(f"Ready: {path}")

SPARK_UI_ACTIVE_MODE:bool = False


# Code
parser = argparse.ArgumentParser()
parser.add_argument("-m", "--month", nargs='?', help="month")
parser.add_argument("-y", "--year", nargs='?', help="year")
parser.add_argument("-s", "--start", nargs='?', help="start day")
parser.add_argument("-o", "--outfile", nargs='?', help="outfile")
#args = parser.parse_args()
args, unknown = parser.parse_known_args()


"""
In this argument parser will be added the function that 
will try to convert the diferent dates to a specific range 
of months and then read only the necessary ones and prune
the uncessary  cols.
"""



params = {
        # numero de contas criadas no período
        #'newaccounts': 0,

        # numero de projetos euroHPC criadas no período
        #'neweurohpcprojecs': 0,
        # numero de projetos nacionais criadas no período
        #'newnationalprojects': 0,

        'reportPeriod': 0,
        'reportPeriodTrimester': 0,
        'reportPeriodYear': 0,
        # mes do report
        'reportMonth': 0,
        # ano do report
        'reportYear': 0,

        # definicoes da maquina
        'armnodes': 1632,
        'amdnodes': 500,
        'gpunodes': 132,

        'percentaviail': 0.8,
        'eurohpcavail': 0.35,

        'ndays': 0,

        'armusedhours': 0,
        'amdusedhours': 0,
        'gpuusedhours': 0,

        # numero de horas utilizadas pelos jobs de projetos eurohpc
        'gpuusedhoursEuroHPC': 0,
        'amdusedhoursEuroHPC': 0,
        'armusedhoursEuroHPC': 0,

        # numero de jobs
        'armJobs': 0,
        'amdJobs': 0,
        'gpuJobs': 0,

        # numero de jobs concluidos com sucesso
        # numero de jobs que falharam
        'gpuCompletedJobs': 0,
        'gpuFailedJobs': 0,
        'armCompletedJobs': 0,
        'amdCompletedJobs': 0,
        'amdFailedJobs': 0,
        'armFailedJobs': 0,

        # numero de jobs concluidos com sucesso de projetos EuroHPC
        'gpuJobsEuroHPC': 0,
        'amdJobsEuroHPC': 0,
        'armJobsEuroHPC': 0,

        'ndaysTrimester': 0,

        'gpuCompletedJobsTrimester': 0,
        'gpuFailedJobsTrimester': 0,
        'armCompletedJobsTrimester': 0,
        'amdCompletedJobsTrimester': 0,
        'amdFailedJobsTrimester': 0,
        'armFailedJobsTrimester': 0,

        'armusedhoursTrimester': 0,
        'amdusedhoursTrimester': 0,
        'gpuusedhoursTrimester': 0,
        'armJobsTrimester': 0,
        'amdJobsTrimester': 0,
        'gpuJobsTrimester': 0,

        'gpuusedhoursEuroHPCTrimester': 0,
        'amdusedhoursEuroHPCTrimester': 0,
        'armusedhoursEuroHPCTrimester': 0,

        'armusedhoursTrimester': 0,
        'amdusedhoursTrimester': 0,
        'gpuusedhoursTrimester': 0,

        'gpuJobsEuroHPCTrimester': 0,
        'amdJobsEuroHPCTrimester': 0,
        'armJobsEuroHPCTrimester': 0,

        # numero de horas utilizadas pelos jobs de projetos eurohpc
        # 'armusedhoursEuroHPCTrimester': 0,
        # 'amdusedhoursEuroHPCTrimester': 0,
        # 'gpuusedhoursEuroHPCTrimester': 0,

        # numero de jobs
        # 'gpuJobsTrimester{ 6515 }
        # 'armJobsTrimester{ 18821 }
        # 'amdJobsTrimester{ 83036 }
        # numero de jobs concluidos com sucesso
        # 'gpuCompletedJobsTrimester{ 4122 }
        # 'armCompletedJobsTrimester{ 15498 }
        # 'amdCompletedJobsTrimester{ 59670 }
        # numero de jobs que falharam
        # 'gpuFailedJobsTrimester{ 671 }
        # 'armFailedJobsTrimester{ 1372 }
        # 'amdFailedJobsTrimester{ 12715 }

        # numero de jobs concluidos com sucesso de projetos EuroHPC
        # 'armuJobsEuroHPCTrimester{ 94 }
        ##'amdJobsEuroHPCTrimester{ 694 }
        # 'gpuJobsEuroHPCTrimester{ 144 }

        'ndaysYear': 0,

        'gpuCompletedJobsYear': 0,
        'gpuFailedJobsYear': 0,
        'armCompletedJobsYear': 0,
        'amdCompletedJobsYear': 0,
        'amdFailedJobsYear': 0,
        'armFailedJobsYear': 0,

        'armusedhoursYear': 0,
        'amdusedhoursYear': 0,
        'gpuusedhoursYear': 0,
        'armJobsYear': 0,
        'amdJobsYear': 0,
        'gpuJobsYear': 0,

        'gpuJobsEuroHPCYear': 0,
        'amdJobsEuroHPCYear': 0,
        'armJobsEuroHPCYear': 0,

        'gpuusedhoursEuroHPCYear': 0,
        'amdusedhoursEuroHPCYear': 0,
        'armusedhoursEuroHPCYear': 0,



        'monthhours': '{\inteval{\\ndays * 24}}',
        'hoursTrimester': '{\inteval{\\ndaysTrimester * 24}}',
        'hoursYear': '{\inteval{\\ndaysYear * 24}}'

    }

 
# # Mapping All the data with the dates and firing up the file with apend mode:

list_of_Months = list(calendar.month_name)[1:]
list_of_months_abr = list(calendar.month_abbr)[1:]
today = datetime.now().date()
year = today.year
month_int = today.month -2
month = list_of_months_abr[month_int]
syear = date(year, 1, 1)

print(f"MONTH : {month} {month_int} \n {list_of_months_abr}")

if args.month != None:
    month_int = list_of_months_abr.index(args.month)
    month = list_of_months_abr[month_int]
    print(f"MONTH2 : {month} {month_int} ")
    
if args.year != None:
    year = int(args.year)
    syear = date(year, 1, 1)
    
if args.start != None:
    syear = datetime.strptime(args.start, "%Y-%m-%d").date()
    
params['reportMonth'] = list_of_Months[month_int]
params['reportYear'] = year
smonth = date(year, month_int+1, 1)
emonthd = smonth + relativedelta(months=1) + relativedelta(days=-1)
#emonthd = date(year, month_int+2,1) - timedelta(days=1)
emonth = smonth + relativedelta(months=1)
#emonth = date(year, month_int + 2, 1)

if month_int < 3:
    tmonth = emonth - relativedelta(months= month_int+1)
else:
    tmonth = emonth - relativedelta(months = 3)
    
print(f"smonth {smonth} -- emonthd {emonthd} -- emonth {emonth} -- tmonth {tmonth}")
params['reportPeriod'] = f"{smonth.strftime('%d/%m/%Y')} - {emonthd.strftime('%d/%m/%Y')}"

if month_int < 3:
    params['reportPeriodTrimester'] = f"{syear.strftime('%d/%m/%Y')} - {emonthd.strftime('%d/%m/%Y')}"
else:
    params['reportPeriodTrimester'] = f"{tmonth.strftime('%d/%m/%Y')} - {emonthd.strftime('%d/%m/%Y')}"

params['reportPeriodYear'] = f"{syear.strftime('%d/%m/%Y')} - {emonthd.strftime('%d/%m/%Y')}"
params['ndays'] = (emonth-smonth).days
params['ndaysTrimester'] = (emonth - tmonth).days
params['ndaysYear'] = (emonth - syear).days

tag_month={
    '': [month,],
    'Trimester': None,
    'Year': list_of_months_abr[:month_int+1]
}

if month_int < 3:
    tag_month['Trimester'] = list_of_months_abr[:month_int+1]
else:
    tag_month['Trimester'] = list_of_months_abr[month_int-2:month_int+1]
    
outfilename = "params.tex"

if args.outfile != None:
    outfilename = args.outfile
    
outfile_path = Path(f"{OUTDIR}/{outfilename}")  

if not outfile_path.is_file():
    outfile_path.parent.mkdir(parents=True, exist_ok=True)
    outfile_path.write_text("", encoding='utf-8') 

wfile = open(f"{outfile_path}","w+")


# # Spark Part


# Extracting the data from params['reportPeriod'] = f"{smonth.strftime('%d/%m/%Y')} - {emonthd.strftime('%d/%m/%Y')}":

start_period: date = datetime.strptime(params['reportPeriod'].split(" - ")[0], "%d/%m/%Y").date()
end_period: date = datetime.strptime(params['reportPeriod'].split(" - ")[1], "%d/%m/%Y").date()

years_to_process_months: dict[int, list[str]] = {} #year: [months_to_process]

current_iteration = date(start_period.year, start_period.month, 1)
while current_iteration <= end_period:
    
    if current_iteration.year not in years_to_process_months:
        years_to_process_months[current_iteration.year] = []
        
    m_abr:str = current_iteration.strftime('%b')
    years_to_process_months[current_iteration.year].append(m_abr)
    
    current_iteration += relativedelta(months=1)

"""
print("PARAMS:")
print(f"Start period: {start_period} -- End period: {end_period}")
for year, months in years_to_process_months.items():
    print(f"Year: {year} - Months: {months}")
"""
    

print("FIND SPARK")
print(findspark.find())

sc = (SparkSession.builder
      .master("local[*]")
      .config("spark.eventLog.enabled", "true")
      .config("spark.eventLog.dir", f"{SPARK_EVENT_LOG_DIR}")
      #.config("executor.memory", "4g")
      #.config("num.executors", "4")
      .config("spark.jars.packages", "io.dataflint:dataflint-spark4_2.13:0.8.5")
      .config("spark.plugins", "io.dataflint.spark.SparkDataflintPlugin")
      .getOrCreate()
      )


nd = None
for year, months in years_to_process_months.items():

    for root, dirs, files in os.walk(f"{DATADIR}/{year}"):  
    
        for f in files:
        
            month_file = "_".join(f.split("_")[1:]).split(".")[0]
        
            print(f"Checking file: {f} for month: {month_file} in months: {months}")
            
            if month_file in months:
                full_path = f"{DATADIR}/{year}/{f}"
                
                print(f"Process: {month_file} in {full_path}")
                
                #data = sc.read.option("delimiter","|").csv(full_path, inferSchema=True, header=True)
                
                data = sc.read.option("delimiter","|")\
                    .csv(f'{DATADIR}/{year}/{f}', 
                         inferSchema = True, 
                         header = True)\
                    .select("ElapsedRaw", "Account", "AllocCPUS", "NNodes", "Partition", "State", "AllocTRES")
                
                data = data.withColumn('Period', F.lit(month))
                
                if nd is None:
                    nd = data
                else:
                    nd = nd.union(data)

tag = ""

#nd.describe()
#adicionar coluna cluster com valores ARM, AMD, GPU
#Adicionar coluna Agency com valores FCT, EHPC, LOCAL
#Adicionar coluna NNodes com o numero de nodos alocados por causa de os nó GPU não ser exclusivo
#Forma do calcular os nós usados nos jobs com GPU
#Adicionar coluna totalJobSeconds = ElapsedRaw * NNodes


# SQL Version:
nd.createOrReplaceTempView("prune_data")

query = """--sql
    WITH calculated_cols AS (
        SELECT 
            -- Original Columns
            ElapsedRaw, Account, AllocCPUS, NNodes, Partition, State, AllocTRES, Period,
            
            -- EState: Cleanup the CANCELLED state suffix
            regexp_replace(State, 'CANCELLED(.*)', 'CANCELLED') AS EState,
            
            -- COMPLETED: Binary status flag
            CASE 
                WHEN State = 'COMPLETED' THEN 'COMPLETED' 
                ELSE 'FAILED' 
            END AS COMPLETED,
            
            -- cluster: Partition type mapping
            CASE 
                WHEN Partition LIKE '%arm%' THEN 'ARM'
                WHEN Partition LIKE '%a100%' THEN 'GPU'
                ELSE 'AMD' 
            END AS cluster,
            
            -- Agency: Account and funding source mapping
            CASE 
                WHEN Account LIKE 'f%' THEN 'FCT'
                WHEN Account LIKE 'ee%' THEN 'EHPC'
                ELSE 'LOCAL' 
            END AS Agency,
            
            -- OldVNodes: Rounding logic for CPU/32 ratio
            CASE 
                WHEN Partition LIKE '%a100%' THEN CEIL(AllocCPUS / 32)
                ELSE NNodes 
            END AS OldVNodes,
            
            -- VNodes: Complex GPU allocation logic (gres/gpu extraction)
            CASE 
                WHEN Partition LIKE '%a100%' THEN 
                    CASE 
                        WHEN AllocTRES IS NULL THEN NNodes
                        WHEN AllocTRES RLIKE 'gres/gpu=([0-9]+)' 
                            THEN CAST(regexp_extract(AllocTRES, 'gres/gpu=([0-9]+)', 1) AS INT)
                        ELSE NNodes * 4 
                    END
                ELSE NNodes 
            END AS VNodes
        FROM prune_data
    )
    SELECT 
        *, 
        -- Final calculation using the virtual VNodes column defined in the CTE
        (ElapsedRaw * VNodes) AS totalJobSeconds
    FROM calculated_cols
"""


## df_result = sc.sql(query)
nd = sc.sql(query)
 
#nd.groupby('EState').count().show()



print(f"1. {nd.count()}")
nd.show()

cl = ['ARM', 'AMD', 'GPU']

 
# # Write to the file:


for tag,months in tag_month.items():
    print(f"{tag} {months}")
    hours = dict()
    jobs = dict()
    completed = nd.filter(F.col('Period').isin(months)).groupby( 'COMPLETED', 'cluster').count().collect()
    msg =''
    for row in completed:
        print(f"ROW: {row}")
        if row.COMPLETED == 'COMPLETED':
            params[f"{row.cluster.lower()}CompletedJobs{tag}"] = row.asDict()['count']
            msg = f"\def\{row.cluster.lower()}CompletedJobs{tag}{{{row.asDict()['count']}}}\n"
        else:
            params[f"{row.cluster.lower()}FailedJobs{tag}"] = row.asDict()['count']
            msg = f"\def\{row.cluster.lower()}FailedJobs{tag}{{{row.asDict()['count']}}}\n"
        print(f"MSG: {tag} {msg}")
    #ignoring local consumed hours
    
    for c in cl:
        #hours[c] = nd.filter(F.col('Period').isin(months)).groupby("cluster").sum().filter(F.col("cluster") == c).collect()[0].asDict()['sum(ElapsedRaw)']
        hours[c] = \
        nd.filter(F.col("Agency") != 'LOCAL').filter(F.col('Period').isin(months))\
            .groupby("cluster").sum().filter(F.col("cluster") == c)\
            .collect()[0].asDict()['sum(totalJobSeconds)']
        print(f" {months} HOURS {c} {hours[c]}")
        
    #ignoring local consumed hours
    for row in nd.filter(F.col("Agency") != 'LOCAL').filter(F.col('Period').isin(months))\
            .groupby("cluster").count().collect():
        print(f"ROW jobs: {row}")
        r = row.asDict()
        jobs[r['cluster']] = r['count']
    print(f"JOBS: {jobs}")
    
    for k, v in hours.items():
        params[f"{k.lower()}usedhours{tag}"] = v / 3600
        msg = f"\def\{k.lower()}usedhours{tag}{{{v / 3600}}}\n"
        print(f"HOURS {tag} {msg}")
        
    for k, v in jobs.items():
        params[f"{k.lower()}Jobs{tag}"] = v
        msg = f"\def\{k.lower()}Jobs{tag}{{{v}}}\n"
        print(f"JOBS {tag} {msg}")
        
    for row in (nd.filter(F.col('Period').isin(months)).groupby(['Agency', 'cluster'])
            .count().orderBy('Agency').filter(F.col("Agency") == 'EHPC').collect()):
        params[f"{row.cluster.lower()}JobsEuroHPC{tag}"] = row.asDict()['count']
        msg = f"\def\{row.cluster.lower()}JobsEuroHPC{tag}{{{row.asDict()['count']}}}\n"
        print(msg)
        
    rows = (nd.filter(F.col("Agency") == 'EHPC').filter(F.col('Period').isin(months))
            .groupby(['Agency', 'cluster']).sum().collect())
    for row in rows:
    #for row in nd.filter(F.col("Agency") == 'EHPC').filter(F.col('Period').isin(months)).groupby(['Agency', 'cluster']).sum().collect():
        #print(f"EHPC {row} --> {row.cluster.lower()}usedhoursEuroHPC{tag}")
        params[f"{row.cluster.lower()}usedhoursEuroHPC{tag}"] = row.asDict()['sum(totalJobSeconds)'] / 3600
        msg = f"\def\{row.cluster.lower()}usedhoursEuroHPC{tag}{{{row.asDict()['sum(totalJobSeconds)'] / 3600}}}\n"
        print(msg)
        
#wfile.write("%%%%%%%%%%%%%%%%%%%%%%%%\n")


## This is the real file writing:
for k, v in params.items():
    msg = f"\def\{k}{{{v}}}\n"
    wfile.write(msg)
wfile.close()


print("SPARK UI ACTIVE MODE")
print(f"SPARK UI URL: {sc.sparkContext.uiWebUrl}")

if SPARK_UI_ACTIVE_MODE:
    input("Press Enter to exit...")


