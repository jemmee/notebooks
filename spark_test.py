# docker network create spark-net
#
# docker run -d \
#  --name spark-master \
#  --hostname spark-master \
#  --network spark-net \
#  -p 8080:8080 -p 7077:7077 \
#  -e SPARK_MODE=master \
#  apache/spark:3.5.0 /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master
#
# docker run -d \
#  --name spark-worker \
#  --network spark-net \
#  -e SPARK_MODE=worker \
#  -e SPARK_MASTER_URL=spark://spark-master:7077 \
#  apache/spark:3.5.0 /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077
#
# docker cp spark_test.py spark-master:/opt/spark/spark_test.py
#
# docker exec -it spark-master /opt/spark/bin/spark-submit \
#  --master spark://spark-master:7077 \
#  /opt/spark/spark_test.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("DockerSparkDemo") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Create dummy data
data = [("Apache Spark is fast",), ("Docker makes Spark easy",), ("Spark on Docker is powerful",)]
df = spark.createDataFrame(data, ["text"])

# Split text into words and count them
word_counts = df.select(explode(split(col("text"), " ")).alias("word")) \
    .groupBy("word") \
    .count()

word_counts.show()
spark.stop()
