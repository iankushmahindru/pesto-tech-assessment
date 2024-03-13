import json
from tokenize import String
from xmlrpc.client import DateTime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
import datetime
import smtplib
from email.mime.text import MIMEText

# Setup SparkSession
spark = SparkSession.builder.appName("DataIngestion").getOrCreate()

########### Data Ingestion Process #####################

# Schema for JSON data (ad impressions)
json_schema = StructType([
    StructField("ad_id", StringType(), True),
    StructField("campaign_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("update_ts", DateTime(), True),
    StructField("impression_id", StringType(), True),
    StructField("num_impressions", IntegerType(), True),
    # Add more fields as needed
])

# Schema for CSV data (clicks/conversions)
csv_schema = StructType([
    StructField("event_ts", StringType(), True),
    StructField("campaign_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("click_id", StringType(), True),
    StructField("num_clicks", IntegerType(), True),
    StructField("conversion_type", StringType(), True)
    # Add more fields as needed
])

# Schema for Avro data (bid requests)
avro_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("user_name", StringType(), True),
    StructField("user_email", StringType(), True),
    StructField("user_capital", IntegerType(), True),
    StructField("campaign_id", StringType(), True),
    StructField("auction_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("initial_price", IntegerType(), True),
    StructField("bid_price", IntegerType(), True),
    StructField("sold_price", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("update_ts", DateTime(), True)
])

# Read JSON data (ad impressions)
json_df = spark.read.json("/mnt/deltalake/impressions.json", multiline=True, schema=json_schema)

# Read CSV data (clicks/conversions)
csv_df = spark.read.csv("/mnt/deltalake/clicksPerConversion.csv", header=True, schema=csv_schema)

# Read Avro data (bid requests)
avro_df = spark.read.format("avro").load("/mnt/deltalake/requests.avro", schema=avro_schema)

########### Data Processing Process #####################

nullcounts = json_df.select([count(when(col(c).isNull(), c)).alias(c) for c in json_df.columns])

if nullcounts != 0:
    # non_null_columns = [col for col in json_df.columns if json_df.select(col).filter(col(col).isNotNull()).count() > 0]
    json_df = json_df.dropna()
    
# Apply filter on json data and csv data
join_condition = ["user_id"]
filtered_df = json_df.join(csv_df,join_condition,"left")

# Calculate total clicks per campaign

avg_clicks = csv_df.groupBy(col("campaign_id")).agg(sum(col("clicks")).alias(col("total_clicks")))

# Calculate average ad impressions per campaign 

avg_impressions = filtered_df.groupBy(col("campaign_id")).agg(avg(col("num_impressions").alias("avg_number_of_imoressions")))

# Calculate total products sold

count_products_sold = avro_df.filter(col("sold_price") > 0).count()
inc_price = avro_df.select((col("sold_price") - col("initial_price")).alias("increment_price"))

########### Data Storage Process #####################

# Storing with Effective storage and query performance

avg_impressions.write.format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").partitionBy("user_id","campaign_id").mode("overwrite").save("/mnt/deltalake/json_data")
avg_clicks.write.format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").partitionBy("user_id","campaign_id").mode("overwrite").save("/mnt/deltalake/csv_data")
avro_df.write.format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").partitionBy("user_id","campaign_id").mode("overwrite").save("/mnt/deltalake/avro_data")

########### Error Handling Process #####################

error_condition = (col("num_impressions") < 0) or (col("num_clicks") < 0)

def sendEmailAlert(subject,message):
    sender = "myemail@domain.com"
    receiver = "supportteam@domain.com"
    subject = subject
    body = message
    smtp_port = 587
    smtp_server = "domain.com"
    smtp_username = "your_smtp_username"
    smtp_password = "your_smtp_password"
    
    msg = MIMEText(message)
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = receiver

    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(smtp_username, smtp_password)
        server.sendmail(sender, receiver, msg.as_string())
        
if filtered_df.filter(error_condition).count() > 5:
    subject = "Data Quality Error"
    message = "Either number of impressions or number of clicks are negative"
    sendEmailAlert(subject=subject, message=message)

# Stop SparkSession
spark.stop()
