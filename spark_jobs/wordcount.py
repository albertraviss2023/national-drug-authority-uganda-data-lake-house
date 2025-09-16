from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WordCount").getOrCreate()
sc = spark.sparkContext
text_data = sc.parallelize([
    "Hello world",
    "Apache Spark is great",
    "Airflow and Spark integration"
])
counts = text_data.flatMap(lambda line: line.split(" ")) \
                  .map(lambda word: (word, 1)) \
                  .reduceByKey(lambda a, b: a + b)
print(counts.collect())
spark.stop()
