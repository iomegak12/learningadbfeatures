// Databricks notebook source
// MAGIC %python
// MAGIC configs = {
// MAGIC 			"fs.azure.account.auth.type": "OAuth",
// MAGIC 			"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
// MAGIC 			"fs.azure.account.oauth2.client.id": "c26e4aa4-2a3e-43a6-b4e1-34d3cbf7f28f",
// MAGIC 			"fs.azure.account.oauth2.client.secret": "rBb+OolPd/5mi.0-jxf05BEnPRBC83YD",
// MAGIC 			"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/381a10df-8e85-43db-86e1-8893b075b027/oauth2/token",
// MAGIC 			"fs.azure.createRemoteFileSystemDuringInitialization": "true"
// MAGIC 		}

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC dbutils.fs.mount(
// MAGIC source = "abfss://data@iomegastoragev4.dfs.core.windows.net/",
// MAGIC mount_point = "/mnt/salesdata",
// MAGIC extra_configs = configs)

// COMMAND ----------

// MAGIC %fs
// MAGIC 
// MAGIC ls /mnt/salesdata

// COMMAND ----------

val fileName = "/mnt/salesdata/ratings-c.csv"
val data = sc.textFile(fileName).mapPartitionsWithIndex(
  (index, iterator) => {
    if(index == 0) {
      iterator.drop(1)
    }
    
    iterator
  }).map(line => {
    val splitted = line.split(",")
  
    (splitted(2).toFloat, 1)
  })

val pdata = data.reduceByKey((value1, value2) => value1 + value2).sortBy(_._2).collect

pdata.reverse.foreach(println)