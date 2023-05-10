# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer
# MAGIC 1. Create a partitioned table; 
# MAGIC 2. Insert values into it using bronze table data.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bees_test_silver.breweries (
# MAGIC   address_1 STRING,
# MAGIC   address_2 STRING,
# MAGIC   address_3 STRING,
# MAGIC   brewery_type STRING,
# MAGIC   id STRING,
# MAGIC   latitude STRING,
# MAGIC   longitude STRING,
# MAGIC   name STRING,
# MAGIC   phone STRING,
# MAGIC   postal_code STRING,
# MAGIC   state_province STRING,
# MAGIC   street STRING,
# MAGIC   website_url STRING
# MAGIC ) PARTITIONED BY (
# MAGIC   country STRING,
# MAGIC   state STRING,
# MAGIC   city STRING 
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE TABLE bees_test_silver.breweries
# MAGIC PARTITION (country, state, city)
# MAGIC  select b.address_1, b.address_2, b.address_3, b.brewery_type, b.id, b.latitude, b.longitude, b.name, b.phone, b.postal_code, b.state_province, b.street, b.website_url, b.country, b.state, b.city from bees_test_bronze.breweries b;

# COMMAND ----------

# MAGIC %sql
# MAGIC show PARTITIONS bees_test_silver.breweries

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from bees_test_silver.breweries;
