# Databricks notebook source
# MAGIC %md
# MAGIC # R workbook to scale h2o package

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-requisite
# MAGIC we need to install the correct sparkling_water jar using maven before initiating an H2o cluster.
# MAGIC Here is the link to download the JAR https://h2o.ai/resources/download/
# MAGIC Selecting the correct spark version and clicking the Maven central tab gives the coordinates that need to be added

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Install the required libraries to start sparkling water cluster in databricks
# MAGIC 
# MAGIC For h2o and sparkling water. please ensure the correct version are installed based on Spark version and R version

# COMMAND ----------

install.packages("sparklyr")
install.packages("RCurl")
#Install H2O 3.36.1.1 (zumbo)
install.packages("h2o", type = "source", repos = "http://h2o-release.s3.amazonaws.com/h2o/rel-zumbo/1/R")

# Install RSparkling 3.36.1.1-1-3.2
install.packages("rsparkling", type = "source", repos = "http://h2o-release.s3.amazonaws.com/sparkling-water/spark-3.2/3.36.1.1-1-3.2/R")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load all the required libraries

# COMMAND ----------

library(SparkR)
library(readr)
library(rsparkling)
library(sparklyr)
# library(sparklyr)


# COMMAND ----------

# # Install H2O 3.36.1.1 (zumbo)
# install.packages("h2o", type = "source", repos = "http://h2o-release.s3.amazonaws.com/h2o/rel-zumbo/1/R")

# Install RSparkling 3.36.1.1-1-3.2
install.packages("rsparkling", type = "source", repos = "http://h2o-release.s3.amazonaws.com/sparkling-water/spark-3.2/3.36.1.1-1-3.2/R")

# COMMAND ----------

sc <- spark_connect(method = "databricks")
h2oConf <- H2OConf()
hc <- H2OContext.getOrCreate(h2oConf)

# COMMAND ----------

df <- createDataFrame (
list(list(1L, 1, "1", 0.1), list(1L, 2, "1", 0.2), list(3L, 3, "3", 0.3)),
  c("a", "b", "c", "d"))

# COMMAND ----------

schema <- structType(structField("a", "integer"), structField("c", "string"),
  structField("avg", "double"))


# COMMAND ----------

result <- gapply(
  df,
  c("a", "c"),
  function(key, x) {
    # key will either be list(1L, '1') (for the group where a=1L,c='1') or
    #   list(3L, '3') (for the group where a=3L,c='3')
    y <- data.frame(key, mean(x$b), stringsAsFactors = FALSE)
}, schema)

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.notebook.getContext.tags()

# COMMAND ----------


