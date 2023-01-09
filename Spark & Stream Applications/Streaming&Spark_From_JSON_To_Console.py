from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Type the following command in your terminal to run/call the Application :
# spark-submit Streaming&Spark_From_JSON_To_Console.py
# You may drop the JSON files in the SparkStream folder to be continuous run

if __name__ == "__main__" :

    # Initialize the spark session
    spark = SparkSession.builder.appName("Streaming&Spark_From_JSON_To_Console").getOrCreate()

    # Define the Schema and create the Data Frame 
    json_schema = " name STRING , post STRING , date INT"
    df = spark.readStream.json("/home/User/SparkStream/" , schema = json_schema )


    # Define the Temp Directory
    directory = "/home/User/temp/"

    # Write the Streamming Data as console format defining the Trigger time and check point
    mycall = df.writeStream.format("console").outputMode("append").trigger(processingTime = "5 second").option("checkpointlocation", directory).start()

    # Ends the program when all the tasks have been finished
    mycall.awaitTermination()