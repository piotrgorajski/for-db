# Databricks notebook source
# MAGIC %md # Prepare flight data in Delta format

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/flights
# MAGIC dmjklsndlks

# COMMAND ----------

dd = spark.read.option("header", True).csv("/databricks-datasets/flights/departuredelays.csv")


# COMMAND ----------

dd.filter("date = 01011245").write.format("delta").mode("overwrite").save("/mnt/mount/delta-intro/dd/")

# COMMAND ----------

# MAGIC %md # Prepare Delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS departuredelays;
# MAGIC CREATE TABLE departuredelays USING DELTA LOCATION '/mnt/mount/delta-intro/dd/';
# MAGIC SELECT * FROM departuredelays;

# COMMAND ----------

# MAGIC %sql DESCRIBE HISTORY departuredelays

# COMMAND ----------

# MAGIC %md # Append more data and investigate

# COMMAND ----------

dd.filter("date = 01020600").write.format("delta").mode("append").save("/mnt/mount/delta-intro/dd/")

# COMMAND ----------

# MAGIC %sql DESCRIBE HISTORY departuredelays

# COMMAND ----------

# MAGIC %md # Perform Time Travel

# COMMAND ----------

# MAGIC %sql SELECT count(*) FROM departuredelays

# COMMAND ----------

# MAGIC %sql SELECT count(*) FROM departuredelays TIMESTAMP AS OF '2021-10-21T15:00:00.000Z'

# COMMAND ----------

# MAGIC %sql SELECT count(*) FROM departuredelays VERSION AS OF 0

# COMMAND ----------

# MAGIC %md # Remove our data

# COMMAND ----------

dbutils.fs.rm("/mnt/mount/delta-intro/dd", recurse=True)
