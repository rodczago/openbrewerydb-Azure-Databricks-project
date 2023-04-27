# Databricks notebook source
# MAGIC %md
# MAGIC # Create Databases for Layers (Bronze, Silver and Gold)
# MAGIC 1. Creation of databases.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS bees_test_bronze
# MAGIC LOCATION '/mnt/zagoadls/bronze/bees_test_bronze'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS bees_test_silver
# MAGIC LOCATION '/mnt/zagoadls/silver/bees_test_silver'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS bees_test_gold
# MAGIC LOCATION '/mnt/zagoadls/gold/bees_test_gold'
