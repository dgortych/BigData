package Lab11

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc, lit, regexp_replace}

object MainHappy extends App {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = SparkSession.builder.master("local[4]")
    .appName("Moja-applikacja")
    .getOrCreate()

  val happinessDF = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/data/happy.csv")

  happinessDF.printSchema()
  happinessDF.show()

  happinessDF.filter(col("Country") === lit("Poland")).show()

  val happinessDfClean = happinessDF.withColumn("Happiness_Score_clean",
    regexp_replace(col("Happiness Score"), ",", ".").cast(sql.types.DoubleType))

  val topSaddest = happinessDfClean.groupBy(col("Country"))
    .mean("Happiness_Score_clean")
    .orderBy(col("avg(Happiness_Score_clean)"))

  val topHappiest = happinessDfClean.groupBy(col("Country"))
    .mean("Happiness_Score_clean")
    .orderBy(desc("avg(Happiness_Score_clean)"))


  topSaddest.show(5)
  topHappiest.show(5)
}
