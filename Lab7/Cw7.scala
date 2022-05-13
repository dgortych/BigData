// Databricks notebook source
// MAGIC %md 
// MAGIC #Zad1
// MAGIC 
// MAGIC Obecnie Hive nie wspiera indeksowania od wersji 3.0. 
// MAGIC W przypadku potrzeby szybkiego wyciągnięcia danych można użyć:
// MAGIC 1. 'zmaterializowane widowki' z automatycznym przepisywaniem
// MAGIC 2. użycie formatów kolumnowych np. Parquet,ORC

// COMMAND ----------

// MAGIC %md
// MAGIC #Zad3

// COMMAND ----------

display(spark.catalog.listDatabases())


// COMMAND ----------

spark.sql("CREATE DATABASE lab7")

// COMMAND ----------

display(spark.catalog.listDatabases())

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/Files/names.csv"
val df = spark.read.format("csv")
    .option("header","true")
    .option("inferSchema","true")
    .load(filePath)

df.write.mode("overwrite").saveAsTable("lab7.names")

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/Files/actors.csv"
val df2 = spark.read.format("csv")
    .option("header","true")
    .option("inferSchema","true")
    .load(filePath)

df2.write.mode("overwrite").saveAsTable("lab7.actors")


// COMMAND ----------

display(spark.catalog.listTables("lab7"))

// COMMAND ----------

// MAGIC %python
// MAGIC def drop_tables(database):
// MAGIC     tables = [table.name for table in spark.catalog.listTables(database)]
// MAGIC     for t in tables:
// MAGIC         spark.sql(f"DELETE FROM {database}.{t}")     
// MAGIC 
// MAGIC drop_tables("lab7")
