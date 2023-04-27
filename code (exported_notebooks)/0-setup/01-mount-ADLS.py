# Databricks notebook source
# MAGIC %md
# MAGIC # Create Azure Data Lake Storage connection
# MAGIC ## Azure pre-reqs
# MAGIC 1. Create app registration;
# MAGIC 2. Grant Storage Blob Data Contributor to it;
# MAGIC 3. Create certificate and copy Secret Key Value.
# MAGIC 
# MAGIC ## Databricks
# MAGIC 1. Variables declaration
# MAGIC 2. Create function to mount ADLS;
# MAGIC 3. Call function to mount FS. 

# COMMAND ----------

storage_account_name = ""      # data lake storage name
client_id            = ""      # app registration client_id
tenant_id            = ""      # app registration tenant_id
client_secret        = ""      # app registration secret key value

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

def mount_adls(container_name):
    if not any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.mount(
            source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
            mount_point = f"/mnt/{storage_account_name}/{container_name}",
            extra_configs = configs)
        print("Directory Mounted Successfully")
    else:
        print("Directory is Already Mounted")

# COMMAND ----------

mount_adls("bronze")

# COMMAND ----------

mount_adls("silver")

# COMMAND ----------

mount_adls("gold")
