#!/usr/bin/env python
# coding: utf-8
import sys

import findspark
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import argparse
import calendar
import os
from datetime import date, timedelta, datetime, time
from dateutil.relativedelta import relativedelta

#findspark.init('/opt/homebrew/Cellar/apache-spark/3.5.5')
findspark.init()

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--month", nargs='?', help="month")
    parser.add_argument("-y", "--year", nargs='?', help="year")
    parser.add_argument("-s", "--start", nargs='?', help="start day")
    parser.add_argument("-o", "--outfile", nargs='?', help="outfile")
    args = parser.parse_args()

    DATADIR = '/projects/F202500010HPCVLABUMINHO/DataSets/Reports/2025'
    OUTDIR  = '/projects/F202500010HPCVLABUMINHO/<YOURFOLDER>/DATA'

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
    wfile = open(f"{OUTDIR}/{outfilename}","w+")



    #
    print("FIND SPARK")
    print(findspark.find())

    sc = (SparkSession.builder
          .config("spark.eventLog.enabled", "true")
          .config("executor.memory", "4g")
          .config("num.executors", "4")
          .config("spark.eventLog.dir", f"file:///projects/F202500010HPCVLABUMINHO/<YOURFOLDER>/spark-events")
          .getOrCreate()
          )


    #load all files from DATADIR starting with jobs_*
    nd = None
    for root, dirs, files in os.walk(DATADIR):
        for f in files:
            print(f)
            if f.startswith('jobs'):
                month = "_".join(f.split("_")[1:]).split(".")[0]
                print(f"Process: {month} {DATADIR}/{f}")
                data = sc.read.option("delimiter","|").csv(f'{DATADIR}/{f}', inferSchema = True, header = True)
                data = data\
                    .withColumn('EState', F.regexp_replace(F.col('State'), "CANCELLED(.*)", "CANCELLED")) \
                    .withColumn('COMPLETED', F.when( F.col('State') == 'COMPLETED' , "COMPLETED").otherwise("FAILED"))
                data = data.withColumn('Period', F.lit(month))
                if nd == None:
                    nd = data
                else:
                    nd = nd.union(data)
    tag = ""
    #nd.describe()
    #adicionar coluna cluster com valores ARM, AMD, GPU
    nd = nd.withColumn("cluster",
        F.when(
            F.col('Partition').contains("arm"), "ARM"
        ).otherwise(
            F.when( F.col('Partition').contains("a100"), "GPU"
            ).otherwise(
            "AMD"
            )
        )
    )

    #Adicionar coluna Agency com valores FCT, EHPC, LOCAL
    nd = nd.withColumn("Agency",
                        F.when(
                            F.col('Account').startswith("f"), "FCT"
                        ).otherwise(
                            F.when(
                                F.col('Account').startswith("ee"), "EHPC"
                            ).otherwise("LOCAL")
                        ))

    #Adicionar coluna NNodes com o numero de nodos alocados por causa de os nó GPU não ser exclusivo
    nd = nd.withColumn("OldVNodes", F.when(
             F.col("Partition").contains("a100"),
                F.when(
                    F.col('AllocCPUS') % 32 == 0,
                            (F.cast(int , F.col('AllocCPUS')/32))
                    ).otherwise(
                            (F.cast(int , F.col('AllocCPUS')/32)+1))
        ).otherwise(F.col("NNodes")))

    #Forma do calcular os nós usados nos jobs com GPU
    nd = nd.withColumn("VNodes", F.when(
             F.col("Partition").contains("a100"),
                F.when(
                    F.col("AllocTRES").isNull(),
                        F.col("NNodes")
                ).otherwise(
                        F.when(F.col("AllocTRES").rlike( r"gres/gpu=(\d+)") ,
                            F.regexp_extract(F.col("AllocTRES"), r"gres/gpu=(\d+)", 1)
                        ).otherwise(
                            F.col("NNodes")*4 #antes de ter este valor usava todo o nó
                        )
                )
             ).otherwise(
                    F.col("NNodes")
            )
        )

    #Adicionar coluna totalJobSeconds = ElapsedRaw * NNodes
    nd = nd.withColumn("totalJobSeconds",
                       (F.col('ElapsedRaw')) * F.col('VNodes')
                       )

    #                   F.regexp_extract(F.col("AllocTRES"), r"gres/gpu=(\d+)", 1))
    #nd = nd.withColumn("totalJobSeconds",
    #                   (F.col('ElapsedRaw') ) * F.col('NNodes')
    #                   )
    #nd.show()
    cl = ['ARM', 'AMD', 'GPU']
    nd.groupby('EState').count().show()
    print(f"1. {nd.count()}")
    nd.show()


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
            #x=wfile.write(msg)
            #print(f"write {x}")
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
            #x = wfile.write(msg)
            #print(f"write {x}")
        for k, v in jobs.items():
            params[f"{k.lower()}Jobs{tag}"] = v
            msg = f"\def\{k.lower()}Jobs{tag}{{{v}}}\n"
            print(f"JOBS {tag} {msg}")
            #x = wfile.write(msg)
            #print(f"write {x}")

        for row in (nd.filter(F.col('Period').isin(months)).groupby(['Agency', 'cluster'])
                .count().orderBy('Agency').filter(F.col("Agency") == 'EHPC').collect()):
            params[f"{row.cluster.lower()}JobsEuroHPC{tag}"] = row.asDict()['count']
            msg = f"\def\{row.cluster.lower()}JobsEuroHPC{tag}{{{row.asDict()['count']}}}\n"
            print(msg)
            #wfile.write(msg)


        rows = (nd.filter(F.col("Agency") == 'EHPC').filter(F.col('Period').isin(months))
                .groupby(['Agency', 'cluster']).sum().collect())

        for row in rows:
        #for row in nd.filter(F.col("Agency") == 'EHPC').filter(F.col('Period').isin(months)).groupby(['Agency', 'cluster']).sum().collect():
            #print(f"EHPC {row} --> {row.cluster.lower()}usedhoursEuroHPC{tag}")
            params[f"{row.cluster.lower()}usedhoursEuroHPC{tag}"] = row.asDict()['sum(totalJobSeconds)'] / 3600
            msg = f"\def\{row.cluster.lower()}usedhoursEuroHPC{tag}{{{row.asDict()['sum(totalJobSeconds)'] / 3600}}}\n"
            print(msg)
            #wfile.write(msg)

    #wfile.write("%%%%%%%%%%%%%%%%%%%%%%%%\n")
    for k, v in params.items():
        msg = f"\def\{k}{{{v}}}\n"
        wfile.write(msg)
    wfile.close()
