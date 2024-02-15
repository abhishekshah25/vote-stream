from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

if __name__ == "__main__":
  # Initialize Spark Session
  spark = (SparkSession.builder
          .appName("Indian_Elections_2024")  # Application Name
          .master("local[*]") # Use local spark execution with all available cores
          .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0") # Spark-Kafka Integration
          .config("spark.jars","C:\Users\abhis\Desktop\Voting System\postgresql-42.7.1.jar") # PostgreSQL Driver
          .config("spark.sql.adaptive.enabled", "false") # Disable adaptive query execution
          .getOrCreate() 
          )
  
  vote_schema = StructType([
      StructField("voter_id", StringType(), True),
      StructField("candidate_id", StringType(), True),
      StructField("voting_time", TimestampType(), True),
      StructField("voter_name", StringType(), True),
      StructField("party_affiliation", StringType(), True),
      StructField("biography", StringType(), True),
      StructField("campaign_platform", StringType(), True),
      StructField("photo_url", StringType(), True),
      StructField("candidate_name", StringType(), True),
      StructField("date_of_birth", StringType(), True),
      StructField("gender", StringType(), True),
      StructField("nationality", StringType(), True),
      StructField("registration_number", StringType(), True),
      StructField("address", StructType([
          StructField("street", StringType(), True),
          StructField("city", StringType(), True),
          StructField("state", StringType(), True),
          StructField("country", StringType(), True),
          StructField("postcode", StringType(), True)
      ]), True),
      StructField("email", StringType(), True),
      StructField("phone_number", StringType(), True),
      StructField("cell_number", StringType(), True),
      StructField("picture", StringType(), True),
      StructField("registered_age", IntegerType(), True),
      StructField("vote", IntegerType(), True)
  ])

#  Reading data from Kafka and processing it
  votes_df = (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "votes_topic")
            .option("startingOffsets", "earliest") 
            .load() 
            .selectExpr("CAST(value AS STRING)") 
            .select(from_json(col("value"), vote_schema).alias("data")) 
            .select("data.*")
            )   