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
    
# Calling Functions
create_df(eventsDF, "data.*")
unique_record(dataDF, "user_id")



