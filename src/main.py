# Importing required dependencies
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
from pyspark.sql.functions import max
from pyspark.sql.functions import mean
from pyspark.sql.functions import min
import json, sys

# Creating Spark Config and Context
conf = SparkConf().setAppName("GameAnalytics")
sc = SparkContext(conf = conf)

# Configuring Spark Session
spark = SparkSession(sc)

# path of the dataset
path = "C:/game-analytics/data.json"

# Loading the dataset
eventsDF = spark.read.json(sys.argv[1])
#eventsDF = spark.read.json(path)

# Printing the schema
eventsDF.printSchema()

# Creating Functions

# Function to select particular data field from df 
def create_df(df, field):
   global dataDF     
   dataDF = df.select(field)

# Function to find unqiue number of records in a column
def unique_record(df, variable_name):
   global total_unique 
   total_unique = df\
            .select(variable_name)\
            .distinct()\
            .count()
   print("The number of unique records for", variable_name,  "is :", total_unique)
	
# Function to find top 3 records in a column
def top_three(df, column_name):
    global total_top3, manufacturer_to_list
    total_top3 = df\
                .select(column_name)\
                .groupBy(column_name)\
                .count()\
                .orderBy(desc("count"))\
                .limit(3)
    total_top3.show()
    manufacturer_to_list = total_top3.toPandas()[column_name].values.T.tolist()
	
# Function to find min, max and mean of a column)
def min_max_mean_record(df, column_name1, column_name2):
    global session_id_num, session_output, to_json_session, session_parsed
    session_id_num = df\
        .select(column_name1, column_name2)\
        .distinct()
    
    session_output = session_id_num\
        .select(mean(column_name2), min(column_name2), max(column_name2))\
        .withColumnRenamed("avg(" + column_name2 + ")", "mean")\
        .withColumnRenamed("min(" + column_name2 + ")", "min")\
        .withColumnRenamed("max(" + column_name2 + ")", "max")
        
    session_output.show()
    to_json_session = session_output.toJSON().collect()
    to_json_session = ''.join(map(str,to_json_session))
    session_parsed = json.loads(to_json_session)
	
# Function for final output
def output(unique, top3, sessions):

    global d_user_id, d_manufacturers, d_sessions, d_final
    
    d_user_id = dict([("active_users", unique)])
    d_manufacturers = dict([("manufacturers", top3)])
    d_sessions = dict([("sessions", sessions)])
    d_user_id.update(d_manufacturers)
    d_user_id.update(d_sessions)
    d_final = dict(d_user_id)
    print(json.dumps(d_final, indent = 2))
    
# Calling Functions
create_df(eventsDF, "data.*")
unique_record(dataDF, "user_id")
top_three(dataDF, "manufacturer")
min_max_mean_record(dataDF, "session_id", "session_num")
output(total_unique, manufacturer_to_list, session_parsed)



