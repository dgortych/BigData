// Databricks notebook source
val fileName = "dbfs:/FileStore/tables/Nested.json"
val df = spark.read.option("multiline","true")
      .json(fileName)


display(df)

// COMMAND ----------

display(df.select($"pathLinkInfo".dropFields("captureSpecification")))

// COMMAND ----------

import org.apache.spark.sql.functions._
import spark.implicits._

val df2 = Seq(
  ("Ri@!ck$", "Ric@ky"),
  ("B@@b", "B!@#$ooby")
).toDF("name", "nickname")

val dfFixed = Seq(
  "name",
  "nickname"
).foldLeft(df2) { (memoDF, colName) =>
  memoDF.withColumn(
    colName,
    regexp_replace(col(colName), "[#@!$%]", "")
  )
}

dfFixed.show()
