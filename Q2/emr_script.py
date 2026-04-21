from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("Count RDS Records") \
    .getOrCreate()

# RDS MySQL connection details
jdbc_url = "jdbc:mysql://database-1.c9mewsiq2n1k.us-east-1.rds.amazonaws.com:3306/processed_table?useSSL=false&allowPublicKeyRetrieval=true"

properties = {
    "user": "admin",
    "password": "admin1234",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Read table from RDS
df = spark.read.jdbc(
    url=jdbc_url,
    table="(SELECT * FROM processed_table.users_processed) AS tmp",
    properties=properties
)

# Count records
record_count = df.count()

print(f"Total records in users_processed: {record_count}")

# Stop Spark session
spark.stop()