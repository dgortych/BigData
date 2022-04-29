// Databricks notebook source
// MAGIC %md
// MAGIC #Zadanie 1

// COMMAND ----------

val transactions_df = spark.read.table("Sample.Transactions")
val logical_df = spark.read.table("Sample.Logical")

// COMMAND ----------

//%sql
//SELECT AccountId,
//TranDate,
//TranAmt,
//-- running total of all transactions
//SUM(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate) as RunTotalAmt
//FROM Sample.Transactions ORDER BY AccountId, TranDate;

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

val windowSpec1 = Window.partitionBy("AccountId").orderBy("TranDate")  
                                
display( transactions_df.select( $"AccountId",$"TranDate",$"TranAmt",sum("TranAmt").over(windowSpec1) as "RunTotalAmt" ).orderBy($"AccountId",$"TranDate") )

// COMMAND ----------

//%sql
//SELECT AccountId,
//TranDate,
//TranAmt,
//-- running average of all transactions
//AVG(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate) as RunAvg,
//-- running total # of transactions
//COUNT(*) OVER (PARTITION BY AccountId ORDER BY TranDate) as RunTranQty,
//-- smallest of the transactions so far
//MIN(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate) as RunSmallAmt,
//-- largest of the transactions so far
//MAX(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate) as RunLargeAmt,
//-- running total of all transactions
//SUM(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate) RunTotalAmt
//FROM Sample.Transactions 
//ORDER BY AccountId,TranDate;


val windowSpec2 = Window.partitionBy("AccountId").orderBy("TranDate")

display( transactions_df.select( $"AccountId",$"TranDate",$"TranAmt",
                                 avg("TranAmt").over(windowSpec2) as "RunAvg",
                                 count("*").over(windowSpec2) as  "RunTranQty",  
                                 min("TranAmt").over(windowSpec2) as "RunSmallAmt",
                                 max("TranAmt").over(windowSpec2) as "RunLargeAmt",
                                 sum("TranAmt").over(windowSpec2) as "RunTotalAmt" )                              
                                 .orderBy($"AccountId",$"TranDate") )



// COMMAND ----------

//%sql
//SELECT AccountId,
//TranDate,
//TranAmt,
//-- average of the current and previous 2 transactions
//AVG(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as SlideAvg,
//-- total # of the current and previous 2 transactions
//COUNT(*) OVER (PARTITION BY AccountId ORDER BY TranDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as SlideQty,
//-- smallest of the current and previous 2 transactions
//MIN(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as SlideMin,
//-- largest of the current and previous 2 transactions
//MAX(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as SlideMax,
//-- total of the current and previous 2 transactions
//SUM(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as SlideTotal,
//ROW_NUMBER() OVER (PARTITION BY AccountId ORDER BY TranDate) AS RN
//FROM Sample.Transactions 
//ORDER BY AccountId, TranDate, RN

val windowSpec3 = Window.partitionBy("AccountId").orderBy("TranDate").rowsBetween( -2, Window.currentRow)

display( transactions_df.select( $"AccountId",$"TranDate",$"TranAmt",
                                 avg("TranAmt").over(windowSpec3) as "SlideAvg",
                                 count("*").over(windowSpec3) as  "SlideQt",  
                                 min("TranAmt").over(windowSpec3) as "SlideMin",
                                 max("TranAmt").over(windowSpec3) as "SlideMax,",
                                 sum("TranAmt").over(windowSpec3) as "SlideTotal",
                                 row_number().over(windowSpec2) as "RN" )  
                                 .orderBy($"AccountId",$"TranDate",$"RN") )





// COMMAND ----------

//%sql
//SELECT RowID,
//FName,
//Salary,
//SUM(Salary) OVER (ORDER BY Salary ROWS UNBOUNDED PRECEDING) as SumByRows,
//SUM(Salary) OVER (ORDER BY Salary RANGE UNBOUNDED PRECEDING) as SumByRange,
//FROM Sample.Logical
//ORDER BY RowID;

val WindowSpec4 = Window.orderBy("Salary").rowsBetween(Window.unboundedPreceding, Window.currentRow)
val WindowSpec5 = Window.orderBy("salary").rangeBetween(Window.unboundedPreceding, Window.currentRow)

display( logical_df.select( $"RowId",$"FName",$"Salary",
                         sum("salary").over(WindowSpec4) as "SumByRows",
                         sum("salary").over(WindowSpec5) as "SumByRange")
                         .orderBy($"RowID") )


// COMMAND ----------

// MAGIC %md
// MAGIC #Zadanie 2

// COMMAND ----------

val Window_rows = Window.partitionBy("AccountId").orderBy("TranDate").rowsBetween(Window.unboundedPreceding, Window.currentRow)
val Window_range = Window.partitionBy("AccountId").orderBy("TranDate").rangeBetween(Window.unboundedPreceding, Window.currentRow)
val Window_between = Window.partitionBy("AccountId").orderBy("TranDate")rowsBetween( -2, Window.currentRow)


display( transactions_df.select( $"AccountId",$"TranDate",$"TranAmt",
                                 first("TranAmt").over(Window_rows) as "FirstRows",
                                 first("TranAmt").over(Window_range) as "FirstRange",
                                 first("TranAmt").over(Window_between) as "FirstBetween",
                                 last("TranAmt").over(Window_rows) as "LastRows",
                                 last("TranAmt").over(Window_range) as "LastRange",
                                 last("TranAmt").over(Window_between) as "LastBetween")
                                 .orderBy($"AccountId",$"TranDate") )




// COMMAND ----------

display( transactions_df.select( $"AccountId",$"TranDate",$"TranAmt",
                                 lead("TranAmt",1).over(windowSpec2) as "Lead1",
                                 lead("TranAmt",2).over(windowSpec2) as "Lead2",                                
                                 lag("TranAmt",1).over(windowSpec2) as "Lag1",
                                 lag("TranAmt",2).over(windowSpec2) as "Lag2")                                
                                 .orderBy($"AccountId",$"TranDate") )

// COMMAND ----------

display( transactions_df.select( $"AccountId",$"TranDate",$"TranAmt",
                                 row_number().over(windowSpec1) as "RowNumber",                           
                                 dense_rank().over(windowSpec1) as "DenseRank")                                
                                 .orderBy($"AccountId",$"TranDate") )

// COMMAND ----------

// MAGIC %md
// MAGIC #Zadanie 3

// COMMAND ----------

val joinExpression = transactions_df.col("AccountId") === logical_df.col("RowID")

val Left_Semi = transactions_df.join(logical_df, joinExpression,"leftsemi")
Left_Semi.show()
Left_Semi.explain()

// COMMAND ----------

val Left_Anti = transactions_df.join(logical_df, joinExpression,"leftanti")
Left_Anti.show()
Left_Anti.explain()

// COMMAND ----------

// MAGIC %md
// MAGIC #Zadanie 4

// COMMAND ----------

val person = Seq(
    (0, "Bill Chambers", 0, Seq(100)),
    (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
    (2, "Michael Armbrust", 1, Seq(250, 100)),
    (2, "Michael Armbrust", 1, Seq(250, 100))) // zdublowana kolumna
  .toDF("id", "name", "graduate_program", "spark_status")
val graduateProgram = Seq(
    (0, "Masters", "School of Information", "UC Berkeley"),
    (2, "Masters", "EECS", "UC Berkeley"),
    (1, "Ph.D.", "EECS", "UC Berkeley"))
  .toDF("id", "degree", "department", "school")

val joinExpression = person.col("graduate_program") === graduateProgram.col("id")
val Join = person.join(graduateProgram, joinExpression)

display(Join.distinct()) // metoda 1

display(Join.dropDuplicates())  // metoda 2

// COMMAND ----------

// MAGIC %md 
// MAGIC #Zadanie 5

// COMMAND ----------

val broadcast_join = person.join(broadcast(graduateProgram), joinExpression)

broadcast_join.show()
broadcast_join.explain()
