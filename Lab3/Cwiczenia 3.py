# Databricks notebook source
# MAGIC %md Names.csv 
# MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
# MAGIC * Dodaj kolumnę w której wyliczysz wzrost w stopach (feet)
# MAGIC * Odpowiedz na pytanie jakie jest najpopularniesze imię?
# MAGIC * Dodaj kolumnę i policz wiek aktorów 
# MAGIC * Usuń kolumny (bio, death_details)
# MAGIC * Zmień nazwy kolumn - dodaj kapitalizaję i usuń _
# MAGIC * Posortuj dataframe po imieniu rosnąco

# COMMAND ----------

filePath = "dbfs:/FileStore/tables/Files/names.csv"
namesDf = spark.read.format("csv").option("header","true").option("inferSchema","true").load(filePath)

display(namesDf)


# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

names_Df = namesDf.withColumn("epoch",unix_timestamp()).withColumn("meters_to_feet",namesDf.height * 3.281) 
names_Df.explain()


# COMMAND ----------

names = namesDf.select("birth_name").withColumn("birth_name",split(namesDf.birth_name," ")[0])
counted_names = names.groupBy("birth_name").count().sort(col("count").desc())
display(counted_names)

#najpopularniejszym jest John 1052 wystąpienia

# COMMAND ----------

namesDf = namesDf.withColumn( "date_of_birth", when( to_date(namesDf.date_of_birth,"dd.MM.yyyy").cast("date").isNull(), namesDf.date_of_birth ).otherwise( to_date( namesDf.date_of_birth,"dd.MM.yyyy" ).cast("date")))

#with_changed_date = with_changed_date.dropna(subset="changed")

namesDf = namesDf.withColumn("age",( months_between( current_date() , namesDf.date_of_birth ) / 12 ).cast("int")  ) 


# COMMAND ----------

namesDf = namesDf.drop("bio","death_details")
namesDf.printSchema()

# COMMAND ----------

for col in namesDf.columns:
  namesDf = namesDf.withColumnRenamed( col , col.replace("_"," ").title().replace(" ","") )
  
namesDf.printSchema()
   

# COMMAND ----------

namesDf = namesDf.sort(namesDf.Name)
display(namesDf)

# COMMAND ----------

# MAGIC %md Movies.csv
# MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
# MAGIC * Dodaj kolumnę która wylicza ile lat upłynęło od publikacji filmu
# MAGIC * Dodaj kolumnę która pokaże budżet filmu jako wartość numeryczną, (trzeba usunac znaki walut)
# MAGIC * Usuń wiersze z dataframe gdzie wartości są null

# COMMAND ----------

filePath = "dbfs:/FileStore/tables/Files/movies.csv"
moviesDf = spark.read.format("csv").option("header","true").option("inferSchema","true").load(filePath)

display(moviesDf)
moviesDf.printSchema()

# COMMAND ----------

moviesDf = moviesDf.withColumn("epoch",unix_timestamp()) 
moviesDf.select("epoch").show()

# COMMAND ----------

moviesDf = moviesDf.withColumn("changed_date",to_date(moviesDf.date_published,"dd.MM.yyyy")).dropna(subset="changed_date")
moviesDf = moviesDf.withColumn("age",( months_between( current_date() , moviesDf.changed_date ) / 12 ).cast("int")  ) 
display(moviesDf.select("age"))

# COMMAND ----------

moviesDf = moviesDf.withColumn("budget_to_numeric", regexp_replace( moviesDf.budget,"[$a-zA-Z]+","") )
display(moviesDf.select("budget_to_numeric","budget"))

# COMMAND ----------

moviesDf = moviesDf.dropna()

# COMMAND ----------

# MAGIC %md ratings.csv
# MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
# MAGIC * Dla każdego z poniższych wyliczeń nie bierz pod uwagę `nulls` 
# MAGIC * Dodaj nowe kolumny i policz mean i median dla wartości głosów (1 d 10)
# MAGIC * Dla każdej wartości mean i median policz jaka jest różnica między weighted_average_vote
# MAGIC * Kto daje lepsze oceny chłopcy czy dziewczyny dla całego setu
# MAGIC * Dla jednej z kolumn zmień typ danych do `long` 

# COMMAND ----------


filePath = "dbfs:/FileStore/tables/Files/ratings.csv"
ratingsDf = spark.read.format("csv") \
              .option("header","true") \
              .option("inferSchema","true") \
              .load(filePath)

display(ratingsDf)
ratingsDf.printSchema()

# COMMAND ----------

ratingsDf = ratingsDf.withColumn("epoch",unix_timestamp()) 

ratingsDf = ratingsDf.withColumn( "my_mean",  ( ratingsDf.votes_1 * 1 + ratingsDf.votes_2 * 2 + ratingsDf.votes_3 * 3+ ratingsDf.votes_4 * 4+ ratingsDf.votes_5 * 5 + \
                                 ratingsDf.votes_6 * 6+ ratingsDf.votes_7 * 7+ ratingsDf.votes_8 * 8+ ratingsDf.votes_9 * 9+ ratingsDf.votes_10 * 10) / ratingsDf.total_votes )  

display(ratingsDf.select("my_mean","mean_vote"))


# COMMAND ----------

ratingsDf = ratingsDf.withColumn("mean_diff",abs(ratingsDf.weighted_average_vote - ratingsDf.mean_vote) ).withColumn("median_diff",abs(ratingsDf.weighted_average_vote-ratingsDf.median_vote))
display(ratingsDf.select("mean_diff","median_diff"))


# COMMAND ----------

male_avg = ratingsDf.select(avg(ratingsDf.males_allages_avg_vote)).collect()
female_avg = ratingsDf.select(avg(ratingsDf.females_allages_avg_vote)).collect()

if male_avg < female_avg:
  print("Dziewczyny")
elif male_avg > female_avg:
  print("Chłopcy")
else:
  print("Tak samo")

  
ratingsDf = ratingsDf.withColumn("votes_10",ratingsDf.votes_10.cast('long'))
ratingsDf.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #Zad3

# COMMAND ----------

filePath = "dbfs:/FileStore/tables/Files/movies.csv"
moviesDf = spark.read.format("csv") \
              .option("header","true") \
              .option("inferSchema","true") \
              .load(filePath)

moviesDf.select(moviesDf.language,moviesDf.country).explain()

moviesDf.select(moviesDf.language,moviesDf.country).groupBy("country").count().explain()
