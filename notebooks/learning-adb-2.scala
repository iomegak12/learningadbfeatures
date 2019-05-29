// Databricks notebook source
val userName = dbutils.secrets.get(scope = "training-scope", key = "adbclientsecret")

print(userName)

// COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.iomegastoragev4.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.iomegastoragev4.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.iomegastoragev4.dfs.core.windows.net", "c26e4aa4-2a3e-43a6-b4e1-34d3cbf7f28f")
spark.conf.set("fs.azure.account.oauth2.client.secret.iomegastoragev4.dfs.core.windows.net", userName)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.iomegastoragev4.dfs.core.windows.net", "https://login.microsoftonline.com/381a10df-8e85-43db-86e1-8893b075b027/oauth2/token")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
dbutils.fs.ls("abfss://data@iomegastoragev4.dfs.core.windows.net/")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")


// COMMAND ----------

// MAGIC %sh 
// MAGIC wget -P /tmp https://raw.githubusercontent.com/Azure/usql/master/Examples/Samples/Data/json/radiowebsite/small_radio_json.json

// COMMAND ----------

dbutils.fs.cp("file:///tmp/small_radio_json.json", "abfss://data@iomegastoragev4.dfs.core.windows.net/")

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP TABLE IF EXISTS radio_sample_data;
// MAGIC CREATE TABLE radio_sample_data
// MAGIC USING json
// MAGIC OPTIONS (
// MAGIC  path  "abfss://data@iomegastoragev4.dfs.core.windows.net/small_radio_json.json"
// MAGIC )

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * from radio_sample_data