import sys , getopt
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# In your Linux terminal / prompt you may input like this way :
# spark-submit --jars /home/myPOSTGRESdirectory/postgresql-42.2.23.jar Application_PromptRead.py -a /home/Documents/Databricks Study/Data/IBOVESPA/b3_stocks.parquet -t df_ibov
# The -a argument refers to your Data Set directory and the -t argument refers to your postsgres Database name

if __name__ == "__main__" :

    spark = SparkSession.builder.appName("Application_PromptRead").getOrCreate()
    opts , args = getopt.getopt(sys.argv[1:] , "a:t:")
    myfile , table = "","" 

    for opt , arg in opts :
        if opt == "-a" :
            myfile = arg
        elif opt == "-t" :
            table = arg

    df = spark.read.load (myfile)
    df.write.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/-Application_PromptRead").option("dbtable", table).option("user", "postgres").option("password","YOURpassword").option("driver","org.postgresql.Driver").save()
    
    spark.stop()

