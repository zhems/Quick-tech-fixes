import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
# import yaml
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
from dotenv import load_dotenv
from utils import logger # import a class from utils

################### logger func ###################
logger.debug(f'START run of cluster_daily.py on   {datetime.now()}')
################### logger func ###################
load_dotenv()
##########################################################
# with open("/usr/src/config.yaml") as f:    # for docker env
#     var =yaml.load(f, Loader=yaml.FullLoader)

# jars_directory = var['CONNECT']['jars_dir']
jars_directory = os.getenv('jars_dir')
# connector_filepath = var['CONNECT']['connector_path']
connector_filepath = os.getenv('connector_path')
# HOST = var['CONNECT']['host']
HOST = os.getenv("host")
# PORT = var['CONNECT']['port']
PORT = os.getenv('port')
# DATABASE= var['CONNECT']['db']
DATABASE = os.getenv('db')
# COLL_MAP = var['CONNECT']['map_coll']       # DO NOT delete devices!!!!
COLL_MAP = os.getenv('map_coll')
# print("**********************************************************")
# print(f"The jars_directory is: {jars_directory} and of type {type(jars_directory)}")
# print(f"The connector_filepath is: {connector_filepath} and of type {type(connector_filepath)}")
# print(f"The host is: {HOST} and of type {type(HOST)}")
# print(f"The port is: {PORT} and of type {type(PORT)}")
# print(f"The database is: {DATABASE} and of type {type(DATABASE)}")
# print(f"The coll_map is: {COLL_MAP} and of type {type(COLL_MAP)}")
# print("**********************************************************")
# COLL_SOURCE = var['CONNECT']['source_coll'] 
# COLL_SAVED = var['CONNECT']['saved_coll']

#################### wifi 5min Collection to Daily Collection with hourly bars(By CLUSTER) ####################
###### 21 MAY 2024 Wednesday 
COLL_SOURCE="wifiReadings5MinByCluster"   # Check the source coll in mongoDB
COLL_SAVED="wifiReadingsDailyByCluster"  # Check the saved coll in mongoDB
#################### wifi 5min Collection to Daily Collection with hourly bars (By CLUSTER) ####################

# get today datetime for datetime range filtering
today = datetime.now()

print(f"\nINFO: SPARK-SUBMIT cluster_daily.py START {datetime.now()} ---------------------------------\n")
# print("INFO: Take 1hr data to groupby DAY By CLUSTER ... now!\n")

######################### REAL-TIME TESTING BELOW ########################
dt_hour_now = datetime(today.year, today.month, today.day, today.hour)
######################### REAL-TIME TESTING ABOVE ########################


################### Get ISO timestamp END & START below ##################
timezone_str = "+08:00" # SG timezone
# ISO timestamp END
iso_hour_end = dt_hour_now.isoformat() + timezone_str


# Subtract 1 hour from datetime object
dt_substract_1hour = dt_hour_now - relativedelta(hours=1)

# ISO timestamp START
iso_hour_start = dt_substract_1hour.isoformat() + timezone_str

print(f"ISO_hour Cluster START: {iso_hour_start}, END: {iso_hour_end}\n")
################### Get ISO timestamp END & START above ##################


############################ PIPELINE HOURLY CLUSTER BELOW ############################ 
pipeline_hourly_cluster=[
  {
    "$match": {
        "$and": [
          {
            "$expr": {
              "$gte": [
                "$dateTimeStamp",
                { "$dateFromString": {"dateString": iso_hour_start} }   # use string or iso 
              ],
            }
          },
          {
            "$expr": {
              "$lt": [
                "$dateTimeStamp",
                { "$dateFromString": {"dateString": iso_hour_end} },   # use string or iso 
              ],
            }
          },
        ]
      }
  },
  {
    "$unwind": "$deviceData"
  },
  {
    "$group": {
      "_id": {
        "date": {"$dateToString": {"format": "%Y-%m-%dT%H","date": "$dateTimeStamp"}},
        "aClusterId": "$deviceData.aClusterId",
        # "deviceAP": "$deviceData.nameDeviceAP",
      },
      "aLevelId": { "$addToSet": "$deviceData.aLevelId" },
      "buildingId": { "$addToSet": "$deviceData.buildingId" },
      "getHourlyAvgDeviceCount": { "$avg": "$deviceData.deviceCount" },
    }
  },
  {
    "$group": {
      "_id": {
        "timeStamp": { "$toDate": {"$concat": [{ "$substrBytes": ["$_id.date", 0, 13] }]} },
        "aClusterId":"$_id.aClusterId",
      },
      # "aLevelId": { "$addToSet": "$aLevelId" },       # use this for original nested array
      # "buildingId": { "$addToSet": "$buildingId" },   # use this for original nested array   
      "aLevelId": { "$first": "$aLevelId" },          # use this for no nested array
      "buildingId": { "$first": "$buildingId" },      # use this for no nested array
      # "hourlyAvgDeviceCount": { "$first": "$getHourlyAvgDeviceCount" },
      "hourlyAvgDeviceCount": { "$first": { "$round": [ "$getHourlyAvgDeviceCount", 0 ]  } },
      # "deviceAP": { "$first": "$_id.deviceAP" },
    }
  },
  {
    "$group": {
      "_id": "$_id.timeStamp",
      # "_id": { "$dateFromString": {"dateString": iso_hour_start} },
      "deviceData": {
        "$push": {
          "aClusterId": "$_id.aClusterId",
          # "aClusterId": { "$arrayElemAt": ["$_id.aClusterId", 0] },
          "buildingId": { "$arrayElemAt": ["$buildingId", 0] }, 
          "aLevelId": { "$arrayElemAt": ["$aLevelId", 0] },          
          # "nameDeviceAP": "$deviceAP",
          "hourlyAvgDeviceCount": "$hourlyAvgDeviceCount",
        }
      }
    }
  },
  {
    "$project": {
      # "dateTimeStamp": "$_id",
      "dateTimeStamp": { "$dateFromString": {"dateString": iso_hour_start} },
      "deviceData": 1,
      "_id": 0
    }
  },
]  
############################ PIPELINE HOURLY CLUSTER ABOVE ############################ 


####################### SPARK PROCESS PIPELINE BELOW ##############################

# print(f"************************ PYSPARK VERSION: {pyspark.__version__} ************************")

spark = SparkSession.builder.appName("*** PROCESS DAILY CLUSTER ***").\
        config("spark.mongodb.write.convertJson","object_Or_Array_Only").\
        config('spark.driver.extraClassPath', jars_directory).\
        getOrCreate()

spark.sparkContext.setLogLevel('WARN')

df=spark.read.format("mongodb").\
    option("spark.mongodb.read.connection.uri", 
           f"mongodb://{HOST}/{DATABASE}.{COLL_MAP}?readPreference=primaryPreferred").\
    option("spark.mongodb.read.connection.uri", 
           f"mongodb://{HOST}/{DATABASE}.{COLL_SOURCE}?readPreference=primaryPreferred").\
    option("aggregation.pipeline", pipeline_hourly_cluster).\
    load()

df.write.format("mongodb").\
    option("spark.mongodb.write.connection.uri", 
           f"mongodb://{HOST}/{DATABASE}.{COLL_SAVED}?replicaSet=pdd-cluster").\
    mode("append").\
    save()

df.show()
################### logger func ###################
# logger.debug(f'{df.show()}')
################### logger func ###################
# Stop the Spark session
spark.stop()

####################### SPARK PROCESS PIPELINE ABOVE ##############################
