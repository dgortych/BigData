// Databricks notebook source
// Zadanie 1
import org.apache.log4j.{LogManager,Level, Logger}
val log = Logger.getLogger(getClass.getName) 
log.setLevel(Level.WARN) 
logger.info("Tutaj info")
log.warn("Tutaj wiadomosc")  

// COMMAND ----------

// Zadanie 2
// Dane o dystrybucji znajduja sie w executors.

// COMMAND ----------

// Zadanie 3
// Ustawienia: spark.driver.memory, spark.executor.memory
// Można spróbować ustawić małą pamięć i zrobić collect na dużym dataframe

// COMMAND ----------

// Zadanie 4
val database = "lab13_db"
spark.sql(s"CREATE DATABASE IF NOT EXISTS $database")

val df = spark.read
  .format("csv")
  .option("header","true")
  .option("inferSchema","true")
  .load("dbfs:/FileStore/tables/Files/movies.csv")

df.write
  .mode("overwrite")
  .saveAsTable(s"$database.movies")

// COMMAND ----------

df.write
  .format("parquet")
  .mode("overwrite")
  .bucketBy(10, "country")
  .saveAsTable("country_bucketing")

// COMMAND ----------

df.write
  .format("parquet")
  .mode("overwrite")
  .partitionBy("country")
  .saveAsTable("country_partition")

// COMMAND ----------

// Zadanie 5

val path = "dbfs:/FileStore/tables/Files/names.csv"
var namesDf = spark.read
              .format("csv")
              .option("header", "true")
              .option("inferSchema", "true")
              .load(path)


namesDf.summary().show()
