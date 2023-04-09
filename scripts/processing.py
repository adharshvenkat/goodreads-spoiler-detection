from pyspark.sql import SparkSession
# import json

# # loading aws credentials
# with open('../secret.json') as f:
#     data = json.load(f)

# # creating spark session (reading from S3)
# spark = SparkSession.builder \
#     .appName("SpoilerDetection") \
#     .config("spark.hadoop.fs.s3a.access.key", data["aws_access_key_id"]) \
#     .config("spark.hadoop.fs.s3a.secret.key", data["aws_secret_access_key"]) \
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#     .getOrCreate()

# # reading spoiler data from s3
# spoiler_df = spark.read.format("csv").option("header", "true").load("s3a://goodreads-spoiler-detection/raw/")

# # reading book data from s3
# book_df = spark.read.format("csv").option("header", "true").load("s3a://goodreads-spoiler-detection/books/")

# spark session
spark = SparkSession.builder \
    .appName("SpoilerDetection") \
    .getOrCreate()

# reading raw spoiler data from local
raw_spoiler_df = spark.read.format("csv").option("header", "true").load("../data/archive/")

# reading spoiler data from local
spoiler_df = spark.read.format("csv").option("header", "true").load("../data/raw/")

# reading book data from local
book_df = spark.read.format("csv").option("header", "true").load("../data/books/")

# filtering only relevent columns
raw_spoiler_df = raw_spoiler_df.select("review_id", "review_text")
book_df = book_df.select("book_id", "description")

# joining raw spoiler with spoiler and book data
raw_spoiler_df = raw_spoiler_df.repartition("review_id")
book_df = book_df.repartition("book_id")
spoiler_df = spoiler_df.repartition("review_id")

temp_df = spoiler_df.join(raw_spoiler_df, ["review_id"], "left")

spoiler_df = spoiler_df.repartition("book_id")
joined_df = temp_df.join(book_df, ["book_id"], "left") \
    .select("review_text", "rating", "description", "has_spoiler")

# saving joined data to local (parquet) uing partitioning
joined_df.write.mode("overwrite").parquet("../data/processed/")
