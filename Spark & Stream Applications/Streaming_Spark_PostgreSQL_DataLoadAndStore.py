from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# You must initialize the JDBC drive before run this application
# Type the following command in your terminal to run/call the Application :
# spark-submit --jar /home/User/Downloads/postgresql-42.2.23.jar Streaming_Spark_PostgreSQL_DataLoadAndStore.py

# Run the commands below in the Linux terminal to check your data health :
# SELECT * FROM posts ;
# SELECT COUNT(*) FROM posts ;

if __name__ == "__main__" :

    # Define the function to write the postgre data
    def patch_postgre (dataf , batchID) :
        dataf.write.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/-Streaming_Spark_PostgreSQL_DataLoadAndStore").option("dbtable", "posts").option("user", "postgres").option("password","YOURpassword").option("driver","org.postgresql.Driver").mode("append").save()
    
    # Initialize the spark session
    spark = SparkSession.builder.appName("Streaming_Spark_PostgreSQL_DataLoadAndStore").getOrCreate()

    # Define the Schema and create the Data Frame 
    json_schema = " name STRING , post STRING , date INT"
    df = spark.readStream.json("/home/User/SparkPostgreStream/" , schema = json_schema )

    # Define the Temp Directory
    directory = "/home/User/temp/"

    # Stream data write as BATCH format
    mycall = df.writeStream.foreachBatch(patch_postgre).outputMode("append").trigger(processingTime = "5 second").option("checkpointlocation" , directory).start()

    # Ends the program when all the postgre tasks have been finished
    mycall.awaitTermination()

    
   