# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer
# MAGIC 1. Create view table.

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view bees_test_gold.v_breweries_location_quantity as 
# MAGIC select b.brewery_type, b.country, b.state, b.city, count(1) as quantity from bees_test_silver.breweries b
# MAGIC group by b.brewery_type, b.country, b.state, b.city

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bees_test_gold.v_breweries_location_quantity;
