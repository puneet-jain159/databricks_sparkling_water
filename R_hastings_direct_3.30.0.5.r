# Databricks notebook source
# MAGIC %md
# MAGIC # R workbook to scale h2o package by using sparkling water
# MAGIC 
# MAGIC This notebook runs h2o on top of spark cluster

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuring cluster to run sparkling water

# COMMAND ----------

# MAGIC %python
# MAGIC spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pre-requisite
# MAGIC we need to install the correct sparkling_water jar using maven before initiating an H2o cluster. </br>
# MAGIC Here is the link to download the JAR https://h2o.ai/resources/download/ </br>
# MAGIC Selecting the correct spark version and clicking the Maven central tab gives the coordinates that need to be added in the cluster settings tab </br>
# MAGIC <img src="https://github.com/puneet-jain159/databricks_sparkling_water/blob/main/images/sparkling_water.png?raw=true" width="640"/> </br>
# MAGIC Add the cordinates and install the JAR for initiating the cluster
# MAGIC <img src="https://github.com/puneet-jain159/databricks_sparkling_water/blob/main/images/maven.png?raw=true" width="640"/> </br>

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Install the required libraries from CRAN due to version mismatch
# MAGIC 
# MAGIC 1. rlang </br>
# MAGIC 2. cli</br>
# MAGIC 3. Rcurl</br></br>
# MAGIC <img src="https://github.com/puneet-jain159/databricks_sparkling_water/blob/main/images/cran.png?raw=true" width="420"/> </br>
# MAGIC 
# MAGIC 
# MAGIC <img src="https://github.com/puneet-jain159/databricks_sparkling_water/blob/main/images/R_package_list.png?raw=true" width="640"/> </br>

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Install the required libraries to start sparkling water cluster in databricks
# MAGIC 
# MAGIC For h2o and sparkling water. please ensure the correct version are installed based on Spark version and R version

# COMMAND ----------

install.packages("sparklyr")

# COMMAND ----------

#Install H2O 3.36.1.1 (zumbo)
install.packages("h2o", type = "source", repos = "https://h2o-release.s3.amazonaws.com/h2o/rel-zahradnik/5/R")

# Install RSparkling 3.36.1.1-1-3.2
install.packages("rsparkling", type = "source", repos = "http://h2o-release.s3.amazonaws.com/sparkling-water/spark-3.0/3.30.0.5-1-3.0/R")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load all the required libraries

# COMMAND ----------

library(SparkR)
library(readr)
library(rsparkling)
library(sparklyr)
library(h2o)

# COMMAND ----------

sc <- spark_connect(method = "databricks")
h2oConf <- H2OConf()
hc <- H2OContext.getOrCreate(h2oConf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load dummy dataset

# COMMAND ----------

mtcars_tbl <- copy_to(sc, mtcars, overwrite = TRUE)
mtcars_hf <- hc$asH2OFrame(mtcars_tbl)
mtcars_hf

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Train an ML model on the data

# COMMAND ----------

y <- "mpg"
x <- setdiff(names(mtcars_hf), y)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Split the mtcars H2O Frame into train & test sets

# COMMAND ----------


splits <- h2o.splitFrame(mtcars_hf, ratios = 0.7, seed = 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Train a simple GBM Model

# COMMAND ----------

fit <- h2o.gbm(x = x,
               y = y,
               training_frame = splits[[1]],
               min_rows = 1,
               seed = 1)
print(fit)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monitor the predictions

# COMMAND ----------

perf <- h2o.performance(fit, newdata = splits[[2]])
print(perf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Convert back to Spark Dataframe post prediction 

# COMMAND ----------

pred_hf <- h2o.predict(fit, newdata = splits[[2]])
head(pred_hf)
pred_sdf <- hc$asSparkFrame(pred_hf)
head(pred_sdf)

# COMMAND ----------

hc.close()

# COMMAND ----------


