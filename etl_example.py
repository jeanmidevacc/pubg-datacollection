import sys
from datetime import datetime,timedelta

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.context import SparkContext
from pyspark.sql import Row

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

#Setup the Spark context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


#Collect the data
datasource_loggamestateperiodic0 = glueContext.create_dynamic_frame.from_catalog(database = "experiment", table_name = "pubg_rawevents_loggamestateperiodic")
datasource_loggamestateperiodic = Filter.apply(frame = datasource_loggamestateperiodic0, f = lambda x: "-" in x["matchid"])

#Collec the data related to the details of the match (the one from the dynamodb tables)
datasource_detailsmatch = glueContext.create_dynamic_frame.from_catalog(database = "experiment", table_name = "pubg_rawevents_pubg_matchevents_tracker", redshift_tmp_dir = args["TempDir"])

#Build pyspark Dataframe to used the structured API of Spark
dfs_loggamestateperiodic = datasource_loggamestateperiodic.toDF()
dfs_detailmatch = datasource_detailsmatch.toDF()

#Add the details on the match
dfs_loggamestateperiodic = dfs_loggamestateperiodic.join(dfs_detailmatch, ["matchid"])

#Build a function to make some data manipulation (mostly calculate deltatime)
def upgrade_data(x):
    row = x.asDict()

    datetime_tstp = datetime.strptime(row["tstp"],"%Y-%m-%d %H:%M:%S.%f")
    datetime_startdate = datetime.strptime(row["start_date"],"%Y-%m-%d %H:%M:%S")
    datetime_enddate = datetime.strptime(row["end_date"],"%Y-%m-%d %H:%M:%S")

    totalseconds_sincestartdate = (datetime_tstp - datetime_startdate).total_seconds()
    totalseconds_beforeenddate = (datetime_enddate - datetime_tstp).total_seconds()

    row["sincestartmatch_seconds"] = totalseconds_sincestartdate
    row["sincestartmatch_intseconds"] = int(totalseconds_sincestartdate)
    row["sincestartmatch_intminutes"] = int(totalseconds_sincestartdate/60)

    row["beforeendmatch_seconds"] = totalseconds_beforeenddate
    row["beforeendmatch_intseconds"] = int(totalseconds_beforeenddate)
    row["beforeendmatch_intminutes"] = int(totalseconds_beforeenddate/60)

    row["completion_match"] = 100.0*row["gamestate_elapsedtime"]/row["duration"]
    row["part_playersalive"] = 100.0*row["gamestate_numaliveplayers"]/row["nbr_players"]

    return Row(**row)

#Apply the function to the DF
dfs_loggamestateperiodic = dfs_loggamestateperiodic.rdd.map(lambda x:upgrade_data(x)).toDF()

#Convert the data in a Dynamic Frame
datasource0 = DynamicFrame.fromDF(dfs_loggamestateperiodic, glueContext, "datasource0")

#Quick log to check
print "Details on the match", datasource0.count(), datasource0.printSchema()

#Save the data
datasink5 = glueContext.write_dynamic_frame.from_options(frame = datasource0, connection_type = "s3", connection_options = {"path":"s3://glue_etl/events_tables/loggamestateperiodic","partitionKeys": ["partition_0"], "compression": "gzip"}, format = "csv", transformation_ctx = "datasink5")
job.commit()