// Databricks notebook source
// MAGIC %md
// MAGIC ## Jak działa partycjonowanie
// MAGIC 
// MAGIC 1. Rozpocznij z 8 partycjami.
// MAGIC 2. Uruchom kod.
// MAGIC 3. Otwórz **Spark UI**
// MAGIC 4. Sprawdź drugi job (czy są jakieś różnice pomięczy drugim)
// MAGIC 5. Sprawdź **Event Timeline**
// MAGIC 6. Sprawdzaj czas wykonania.
// MAGIC   * Uruchom kilka razy rzeby sprawdzić średni czas wykonania.
// MAGIC 
// MAGIC Powtórz z inną liczbą partycji
// MAGIC * 1 partycja
// MAGIC * 7 partycja
// MAGIC * 9 partycja
// MAGIC * 16 partycja
// MAGIC * 24 partycja
// MAGIC * 96 partycja
// MAGIC * 200 partycja
// MAGIC * 4000 partycja
// MAGIC 
// MAGIC Zastąp `repartition(n)` z `coalesce(n)` używając:
// MAGIC * 6 partycji
// MAGIC * 5 partycji
// MAGIC * 4 partycji
// MAGIC * 3 partycji
// MAGIC * 2 partycji
// MAGIC * 1 partycji
// MAGIC 
// MAGIC ** *Note:* ** *Dane muszą być wystarczająco duże żeby zaobserwować duże różnice z małymi partycjami.*<br/>* To co możesz sprawdzić jak zachowują się małe dane z dużą ilośćia partycji.*

// COMMAND ----------

// val slots = sc.defaultParallelism
spark.conf.get("spark.sql.shuffle.partitions")
spark.catalog.clearCache()

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val schema = StructType(
  List(
    StructField("timestamp", StringType, false),
    StructField("site", StringType, false),
    StructField("requests", IntegerType, false)
  )
)

val fileName = "dbfs:/FileStore/tables/pageviews_by_second.tsv"

var df = spark.read
  .option("header", "true")
  .option("sep", "\t")
  .schema(schema)
  .csv(fileName)

val path = "dbfs:/wikipedia.parquet"

df.write.mode("overwrite").parquet(path)

df.explain
df.count()



// COMMAND ----------

df.show()

// COMMAND ----------

df.rdd.getNumPartitions

// COMMAND ----------

df = spark.read
  .parquet(path)
  .repartition(8)
  .groupBy("site")
  .sum()

// COMMAND ----------

df = spark.read
  .parquet(path)
  .repartition(7)  
  .groupBy("site")
  .sum()

// COMMAND ----------

df = spark.read
  .parquet(path)
  .repartition(9)  
  .groupBy("site")
  .sum()

// COMMAND ----------

df = spark.read
  .parquet(path)
  .repartition(16)  
  .groupBy("site")
  .sum()

// COMMAND ----------

df = spark.read
  .parquet(path)
  .repartition(24)  
  .groupBy("site")
  .sum()

// COMMAND ----------

df = spark.read
  .parquet(path)
  .repartition(96)  
  .groupBy("site")
  .sum()

// COMMAND ----------

df = spark.read
  .parquet(path)
  .repartition(200)  
  .groupBy("site")
  .sum()

// COMMAND ----------

df = spark.read
  .parquet(path)
  .repartition(4000)  
  .groupBy("site")
  .sum()

// COMMAND ----------

df = spark.read
  .parquet(path)
  .coalesce(6)
  .groupBy("site")
  .sum()


// COMMAND ----------

df = spark.read
  .parquet(path)
  .coalesce(5)
  .groupBy("site")
  .sum()


// COMMAND ----------

df = spark.read
  .parquet(path)
  .coalesce(4)
  .groupBy("site")
  .sum()


// COMMAND ----------

df = spark.read
  .parquet(path)
  .coalesce(3)
  .groupBy("site")
  .sum()


// COMMAND ----------

df = spark.read
  .parquet(path)
  .coalesce(2)
  .groupBy("site")
  .sum()


// COMMAND ----------

df = spark.read
  .parquet(path)
  .coalesce(1)
  .groupBy("site")
  .sum()


// COMMAND ----------

// MAGIC %md 
// MAGIC # Zad2

// COMMAND ----------

def read_file(filePath:String): DataFrame = if(Files.exists(Paths.get(filePath))) {
    return spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)
  } else{
  println("No file found")
    return spark.emptyDataFrame
  }


// COMMAND ----------

def fillNaN(colname: String, df :DataFrame): DataFrame = {
   if(df.columns.contains(colname)){
     return df.na.fill(0,Array(colname))
   }
  else
  {
      Logger.getLogger("Such a column does not exist in df").setLevel(Level.ERROR)
      return spark.emptyDataFrame
  }

}

// COMMAND ----------

// MAGIC %python
// MAGIC def dropTables(database:String):
// MAGIC     val tables = [table.name for table in spark.catalog.listTables(database)]
// MAGIC     for t in tables:
// MAGIC         spark.sql(f"DELETE FROM {database}.{t}")     

// COMMAND ----------

 def most_frequent_value(col_name: String): DataFrame ={
    if(df.columns.contains(col_name)){
     return df.select(col(col_name)).withColumn("x", split(col(col_name), " ").getItem(0)).groupBy("x").count().orderBy(desc("count")) 
  }
    else{
      println("Column not in dataframe")
      return spark.emptyDataFrame
    }
  }

// COMMAND ----------

def int_cols(df :DataFrame) : DataFrame = {
  val intCols = df.schema.fields.filter(_.dataType.typeName == "integer")
  if (intCols == spark.emptyDataFrame)
  {
    println( "There is no integer columns!")
    spark.emptyDataFrame    
  }
  else
   df.select(intCols.map(x=>col(x.name)):_*).show()
}

// COMMAND ----------

def fillNaN2(col_names: String, df: String ) DataFrame ={
  for( i <- col_names){
   if(!df.columns.contains(i)){
      println("Column not in dataframe")
      return spark.emptyDataFrame
    } 
    df.na.fill("")
