# Databricks notebook source
# MAGIC %md
# MAGIC #Common Delta Lake Operations

# COMMAND ----------

# Delta Lake is the default for all reads, writes, and table creation commands in Databricks Runtime 8.0 and above.

# Load the data from its source.
df = spark.read.load("/databricks-datasets/learning-spark-v2/people/people-10m.delta")

# Write the data to a table.
table_name = "people_10m"
df.write.saveAsTable(table_name)

# COMMAND ----------

display(spark.sql('DESCRIBE DETAIL people_10m'))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS people10m (
# MAGIC   id INT,
# MAGIC   firstName STRING,
# MAGIC   middleName STRING,
# MAGIC   lastName STRING,
# MAGIC   gender STRING,
# MAGIC   birthDate TIMESTAMP,
# MAGIC   ssn STRING,
# MAGIC   salary INT
# MAGIC );
# MAGIC
# MAGIC CREATE OR REPLACE TABLE people10m (
# MAGIC   id INT,
# MAGIC   firstName STRING,
# MAGIC   middleName STRING,
# MAGIC   lastName STRING,
# MAGIC   gender STRING,
# MAGIC   birthDate TIMESTAMP,
# MAGIC   ssn STRING,
# MAGIC   salary INT
# MAGIC );

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC  -- this works in Databricks Runtime 13.0 and above and can be especially useful when promoting tables from a development environment into production like:
# MAGIC  CREATE TABLE prod.people10m LIKE dev.people10m

# COMMAND ----------

# we may use the DeltaTableBuilder API in Delta Lake to create tables
from delta.tables import DeltaTable

# Create table in the metastore
DeltaTable.createIfNotExists(spark) \
  .tableName("default.people10m") \
  .addColumn("id", "INT") \
  .addColumn("firstName", "STRING") \
  .addColumn("middleName", "STRING") \
  .addColumn("lastName", "STRING", comment = "surname") \
  .addColumn("gender", "STRING") \
  .addColumn("birthDate", "TIMESTAMP") \
  .addColumn("ssn", "STRING") \
  .addColumn("salary", "INT") \
  .execute()

# Create or replace table with path and add properties
DeltaTable.createOrReplace(spark) \
  .addColumn("id", "INT") \
  .addColumn("firstName", "STRING") \
  .addColumn("middleName", "STRING") \
  .addColumn("lastName", "STRING", comment = "surname") \
  .addColumn("gender", "STRING") \
  .addColumn("birthDate", "TIMESTAMP") \
  .addColumn("ssn", "STRING") \
  .addColumn("salary", "INT") \
  .property("description", "table with people data") \
  .location("/tmp/delta/people10m") \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC --Upsert to a table
# MAGIC CREATE OR REPLACE TEMP VIEW people_updates (
# MAGIC   id, firstName, middleName, lastName, gender, birthDate, ssn, salary
# MAGIC ) AS VALUES
# MAGIC   (9999998, 'Billy', 'Tommie', 'Luppitt', 'M', '1992-09-17T04:00:00.000+0000', '953-38-9452', 55250),
# MAGIC   (9999999, 'Elias', 'Cyril', 'Leadbetter', 'M', '1984-05-22T04:00:00.000+0000', '906-51-2137', 48500),
# MAGIC   (10000000, 'Joshua', 'Chas', 'Broggio', 'M', '1968-07-22T04:00:00.000+0000', '988-61-6247', 90000),
# MAGIC   (20000001, 'John', '', 'Doe', 'M', '1978-01-14T04:00:00.000+000', '345-67-8901', 55500),
# MAGIC   (20000002, 'Mary', '', 'Smith', 'F', '1982-10-29T01:00:00.000+000', '456-78-9012', 98250),
# MAGIC   (20000003, 'Jane', '', 'Doe', 'F', '1981-06-25T04:00:00.000+000', '567-89-0123', 89900);
# MAGIC
# MAGIC MERGE INTO people_10m
# MAGIC USING people_updates
# MAGIC ON people_10m.id = people_updates.id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;
# MAGIC
# MAGIC --see the results
# MAGIC SELECT * FROM people_10m WHERE id >= 9999998

# COMMAND ----------

# You access data in Delta tables by the table name or the table path, as shown in the following examples:
people_df = spark.read.table(table_name)

display(people_df)

## or

people_df = spark.read.load(table_path)

display(people_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Delta Lake uses standard syntax for writing data to tables.
# MAGIC -- appending data
# MAGIC INSERT INTO people10m SELECT * FROM more_people
# MAGIC
# MAGIC -- replacing data
# MAGIC INSERT OVERWRITE TABLE people10m SELECT * FROM more_people

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Display table history
# MAGIC DESCRIBE HISTORY people_10m

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query an earlier version of the table (time travel)
# MAGIC SELECT * FROM people_10m VERSION AS OF 0
# MAGIC -- or
# MAGIC SELECT * FROM people_10m TIMESTAMP AS OF '2019-01-29 00:37:58'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optimize a table to deal with large number of small files
# MAGIC OPTIMIZE people_10m
# MAGIC
# MAGIC -- Z-order by columns
# MAGIC OPTIMIZE people_10m
# MAGIC ZORDER BY (gender)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- and finall Clean up the old snapshots
# MAGIC VACUUM people_10m

# COMMAND ----------


