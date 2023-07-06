# Databricks notebook source
arrayData = [
        ('James',['Java','Scala'],{'hair':'black','eye':'brown'}),
        ('Michael',['Spark','Java',None],{'hair':'brown','eye':None}),
        ('Robert',['CSharp',''],{'hair':'red','eye':''}),
        ('Washington',None,None),
        ('Jefferson',['1','2'],{})]


df = spark.createDataFrame(data=arrayData, schema = ['name','knownLanguages','properties'])
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import explode

df2 = df.select(explode(df.knownLanguages).alias("exp_languages"))
df2.printSchema()
df2.show()

# COMMAND ----------

from pyspark.sql.functions import explode

df2 = df.select("*",explode(df.knownLanguages).alias("exp_languages")).drop("knownLanguages")
df2.printSchema()
df2.show(truncate=False)

# COMMAND ----------

df3 = df.select("name",explode(df.knownLanguages).alias("exp_languages"), "properties.eye", "properties.hair")
df3.printSchema()
df3.show(truncate=False)

# COMMAND ----------

df4 = df.select("name", "properties.eye", "properties.hair")
df4.printSchema()
df4.show(truncate=False)
