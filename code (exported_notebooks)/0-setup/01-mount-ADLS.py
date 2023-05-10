# Databricks notebook source
# MAGIC %md
# MAGIC # Create Azure Data Lake Storage connection
# MAGIC ## Azure pre-reqs
# MAGIC 1. Create app registration;
# MAGIC 2. Grant Storage Blob Data Contributor to it;
# MAGIC 3. Create Key Vault and Secret Key Values.
# MAGIC
# MAGIC ## Databricks
# MAGIC 1. Create secret scope to link the keys from Azure Key Vault
# MAGIC 2. Variables declaration
# MAGIC 3. Create function to mount ADLS;
# MAGIC 4. Call function to mount FS. 

# COMMAND ----------

storage_account_name = "zagoadls"
client_id            = dbutils.secrets.get(scope="key-vault-secret",key="appid")
tenant_id            = dbutils.secrets.get(scope="key-vault-secret",key="tenantid")
client_secret        = dbutils.secrets.get(scope="key-vault-secret",key="clientsecret")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": client_id,
           "fs.azure.account.oauth2.client.secret": client_secret,
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.unmount("/mnt/zagoadls/bronze")

# COMMAND ----------

def mount_adls(container_name):
    vmount = f"/mnt/{storage_account_name}/{container_name}"
    if not any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.mount(
            source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
            mount_point = vmount,
            extra_configs = configs)
        print("Directory "+vmount+" Mounted Successfully")
    else:
        print("Directory "+vmount+" is Already Mounted")

# COMMAND ----------

mount_adls("bronze")

# COMMAND ----------

mount_adls("silver")

# COMMAND ----------

mount_adls("gold")
