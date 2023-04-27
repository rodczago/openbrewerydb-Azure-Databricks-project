# Brewery Open DB Project

## Objective:
--------------------
The objective of is to consume data from an API, persist into a data lake architecture with three layers with the first one being raw data, the second one curated and partitioned by location and the third one having analytical aggregated data.
Data lake must follow the medallion architecture having a bronze, silver and gold layer:
 * Bronze layer: Raw / uncurated data usually persisted in its native format
 * Silver: Transformed to columnar storage format such as parquet or delta, partitioned by brewery location
 * Gold: Create an aggregated view with the quantity of stores per type and location


## Used technologies:
-------------------------
1. Azure Databricks (Pyspark, SparkSQL)
2. Delta Lake

# Solution:
---------------

Step 1: Mount Azure Data Lake Storage into Databricks and create Databases.

Step 2: Bronze: Get Data from "https://www.openbrewerydb.org/" and store it into ADLS as Json Format and create Delta Lake Table with its contents.

Step 3: Silver: Create Partitioned Table using Bronze Delta Lake Table created.

Step 4: Gold: Create Aggregated View.

Step 5: Schedule and execute Databricks Notebook with DataBricks Workflows, with the following taks:
1. Mount ADLS;
2. Create Databases * depends on Mount ADLS execution;
3. Bronze Ingestion * depends on Create Databases execution;
4. Silver Filtering * depends on Bronze Ingestion execution;
5. Gold Visualization * depends on Silver Filtering execution.

* The Workflow can be re-executed any time.
