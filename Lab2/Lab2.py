# Databricks notebook source
# MAGIC %md
# MAGIC #Zad1

# COMMAND ----------

#Sprawdzam czy pliki są dostępne w dbfs
display(dbutils.fs.ls("dbfs:/FileStore/tables/Files/"))

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

#stworzenie własnego schematu
myschema = StructType([
  StructField("imdb_title_id", StringType(), True),
  StructField("ordering", ByteType(), True),
  StructField("imdb_name_id", StringType(), True),
  StructField("category", StringType(), True),
  StructField("job", StringType(), True),
  StructField("characters", StringType(), True)]
)

filePath = "dbfs:/FileStore/tables/Files/actors.csv"

#stworzenie Df z uzyciem wlasnego schematu
actorsDf = spark.read.format("csv").option("header","true").schema(myschema).load(filePath) 
           
actorsDf.printSchema()


# COMMAND ----------

# MAGIC %md 
# MAGIC #Zad2

# COMMAND ----------

actorsDf.limit(10).write.mode("overwrite").json("/FileStore/tables/lab2/actors.json")
actors_from_json_df = spark.read.schema(myschema).json("/FileStore/tables/lab2/actors.json")
actors_from_json_df.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC #Zad4

# COMMAND ----------

# MAGIC %scala
# MAGIC //stworzenie wadliwego jsona
# MAGIC 
# MAGIC Seq(
# MAGIC   "{'imdb_title_id':'tt0000009', 'ordering':'1', 'imdb_name_id':'nm0063086', 'category':'actress', 'job':null, 'characters':'[Miss Geraldine Holbrook (Miss Jerry)]'}",
# MAGIC   "{'imdb_title_id':'tt0000009', 'ordering':'2', 'imdb_name_id':'nm0183823', 'category':'actor', 'job':null, 'characters':'[Mr. Hamilton]'}",
# MAGIC   "{'imdb_title_id':'tt0000009', 'ordering':'3', 'imdb_name_idxd:', , , , )}" 
# MAGIC ).toDF().write.mode("overwrite").text("/FileStore/tables/lab2/faulty.json")

# COMMAND ----------

jsonpath = "/FileStore/tables/lab2/faulty.json"

#FAILFAST
df1 = spark.read.format("json") \
  .option("inferSchema","true") \
  .option("mode","FAILFAST") \
  .load(jsonpath)

display(df1)

#opcja nie wykona się

# COMMAND ----------

df2 = spark.read.format("json") \
 .option("inferSchema","true") \
 .option("mode","permissive") \
 .load(jsonpath)

display(df2)

#wartości błędnego wiersza ustawione są na null i jest on przeniesiony do kolumny _corrupt_record

# COMMAND ----------

df3 = spark.read.format("json") \
  .option("inferSchema","true") \
  .option("mode", "DROPMALFORMED") \
  .load(jsonpath)

display(df3)

#błędny wiersz jest usunięty


# COMMAND ----------

df4 = spark.read.format("json") \
  .option("inferSchema","true") \
  .option("badRecordsPath", "/FileStore/tables/lab2/bad_records") \
  .load(jsonpath)

display(df4)

#błędny wiersz jest usunięty i zapisany w pliku o podanej ścieżce

# COMMAND ----------

# MAGIC %md 
# MAGIC #Zad5

# COMMAND ----------

actorsDf.write.format("parquet").mode("overwrite").save("/FileStore/tables/actorsParq.parquet")
actorsParq = spark.read.format("parquet").load("/FileStore/tables/actorsParq.parquet")
display(actorsParq)

#dane są poprawne
