# Databricks notebook source
# MAGIC %md
# MAGIC # Consume Data from API
# MAGIC 1. Variables declaration
# MAGIC 2. API call to store result into a Json file
# MAGIC 3. Read Json file created
# MAGIC 4. Create a delta table with Json content

# COMMAND ----------

filename = 'breweries.json'
folder_path = '/mnt/zagoadls/bronze/BeesTest'
table_name = 'breweries'
database_name = 'bees_test_bronze'

# COMMAND ----------

import json
import requests
import sys
from pyspark.sql.functions import *
import datetime as dt

currentdate = dt.datetime.now()

def get_web_data(url):
    response = requests.get(url);
    responsejson = json.loads(response.text)
    return response.text,responsejson

url = 'https://api.openbrewerydb.org/v1/breweries'

outResult, outResultJson = get_web_data(url)

try:
    dbutils.fs.put(f"{folder_path}/{filename}", outResult, True)
except Exception as e:
    print(str(e))



# COMMAND ----------

df = spark.read.option("multipleline", "true").json(f"{folder_path}/{filename}")

# COMMAND ----------

df.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{database_name}.{table_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC  select * from bees_test_bronze.breweries
