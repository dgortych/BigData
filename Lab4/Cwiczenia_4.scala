// Databricks notebook source
// MAGIC %md 
// MAGIC Wykożystaj dane z bazy 'bidevtestserver.database.windows.net'
// MAGIC ||
// MAGIC |--|
// MAGIC |SalesLT.Customer|
// MAGIC |SalesLT.ProductModel|
// MAGIC |SalesLT.vProductModelCatalogDescription|
// MAGIC |SalesLT.ProductDescription|
// MAGIC |SalesLT.Product|
// MAGIC |SalesLT.ProductModelProductDescription|
// MAGIC |SalesLT.vProductAndDescription|
// MAGIC |SalesLT.ProductCategory|
// MAGIC |SalesLT.vGetAllCategories|
// MAGIC |SalesLT.Address|
// MAGIC |SalesLT.CustomerAddress|
// MAGIC |SalesLT.SalesOrderDetail|
// MAGIC |SalesLT.SalesOrderHeader|

// COMMAND ----------


//INFORMATION_SCHEMA.TABLES

val jdbcHostname = "bidevtestserver.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase = "testdb"

val tabela = spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query","SELECT * FROM INFORMATION_SCHEMA.TABLES")
  .load()

display(tabela)

// COMMAND ----------

// MAGIC %md
// MAGIC 1. Pobierz wszystkie tabele z schematu SalesLt i zapisz lokalnie bez modyfikacji w formacie delta

// COMMAND ----------


import org.apache.spark.sql.functions._

val SalesLT = tabela.where("TABLE_SCHEMA == 'SalesLT'")
val names = SalesLT.select("TABLE_NAME").as[String].collect.toList

var i = List()
for( i <- names){
  val tab = spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query",s"SELECT * FROM SalesLT.$i")
  .load()
  
  tab.write.format("delta").mode("overwrite").saveAsTable(i)
}

// COMMAND ----------

// MAGIC %md
// MAGIC  Uzycie Nulls, fill, drop, replace, i agg
// MAGIC  * W każdej z tabel sprawdź ile jest nulls w rzędach i kolumnach
// MAGIC  * Użyj funkcji fill żeby dodać wartości nie występujące w kolumnach dla wszystkich tabel z null
// MAGIC  * Użyj funkcji drop żeby usunąć nulle, 
// MAGIC  * wybierz 3 dowolne funkcje agregujące i policz dla TaxAmt, Freight, [SalesLT].[SalesOrderHeader]
// MAGIC  * Użyj tabeli [SalesLT].[Product] i pogrupuj według ProductModelId, Color i ProductCategoryID i wylicz 3 wybrane funkcje agg() 
// MAGIC    - Użyj conajmniej dwóch overloded funkcji agregujących np z (Map)

// COMMAND ----------


import org.apache.spark.sql.functions.{col,when, count}
import org.apache.spark.sql.Column

def countCols(columns:Array[String]):Array[Column]={
    columns.map(c=>{
      count(when(col(c).isNull,c)).alias(c)
    })
}

var i = List()
val names2 =names.map(x => x.toLowerCase())
for( i <- names2){
  val filePath = s"dbfs:/user/hive/warehouse/$i"
  val df = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

  df.select(countCols(df.columns):_*).show()
  
}

// COMMAND ----------


for( i <- Tables.map(x => x.toLowerCase())){
  val tab = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(s"dbfs:/user/hive/warehouse/$i")
  val tabNew= tab.na.fill("0", tab.columns)
    display(tabNew)
}

// COMMAND ----------


for( i <- Tables.map(x => x.toLowerCase())){
  val tab = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(s"dbfs:/user/hive/warehouse/$i")
  tab.na.drop("any").show(false)
}

// COMMAND ----------


val filePath = s"dbfs:/user/hive/warehouse/salesorderheader"
val df = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

df.select(approx_count_distinct($"TaxAmt")).show()
df.select(avg($"TaxAmt")).show()
df.select(max($"TaxAmt")).show()

df.select(approx_count_distinct($"Freight")).show()
df.select(avg($"Freight")).show()
df.select(max($"Freight")).show()


// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val filePath = s"dbfs:/user/hive/warehouse/product"
val df = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)
display(df)

df.groupBy($"ProductModelId").mean("StandardCost").show()
df.groupBy($"Color").agg(skewness("Weight")).show()
df.groupBy($"ProductCategoryID").agg(kurtosis("StandardCost")).show()

// COMMAND ----------

// MAGIC %md 
// MAGIC #ZAD2

// COMMAND ----------

import org.apache.spark.sql.types._

val filePath = "dbfs:/FileStore/tables/Files/names.csv"
var namesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath) 

val feet = udf((m: Double) => m * 3.2808)
val mean = udf((s: Integer, v:Integer) => (s+v)/2)
val strLen = udf((s: String) => s.length)

val dfUDF = namesDf.select(feet($"height") as "feet",
            mean($"spouses",$"divorces") as "mean",
            strLen($"name") as "name")
display(dfUDF)

// COMMAND ----------

// MAGIC %
