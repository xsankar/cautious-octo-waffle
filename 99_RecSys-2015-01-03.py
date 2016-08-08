# Databricks notebook source exported at Tue, 17 Mar 2015 19:13:18 UTC
# MAGIC %md ## Session #7 : RecSys Challenge 2015
# MAGIC ### http://2015.recsyschallenge.com/
# MAGIC ### In which we pedict the buying patterns
# MAGIC Version : 1.0

# COMMAND ----------

# Just some header information
import datetime
from pytz import timezone
print "Last run @%s" % (datetime.datetime.now(timezone('US/Pacific')))
print sc.version

# COMMAND ----------

# MAGIC %md ##Learn from others
# MAGIC 1. twitter @ACMRecSys
# MAGIC 2. iPython notebook by @jbochi http://nbviewer.ipython.org/github/jbochi/recsyschallenge2015/blob/master/visualization.ipynb
# MAGIC 3. blogs by @totopampin http://aloneindecember.com/words/recsys-challenge-part-iii/

# COMMAND ----------

# MAGIC %md ###Let us look at the files

# COMMAND ----------

display(dbutils.fs.ls('/mnt/data/recSys-2015/'))

# COMMAND ----------

## Don't do this ! %sql DROP TABLE Clicks

# COMMAND ----------

# MAGIC %sql CREATE TABLE Clicks
# MAGIC USING com.databricks.spark.csv
# MAGIC OPTIONS (path "/mnt/data/recSys-2015/yoochoose-clicks.dat", header "false")

# COMMAND ----------

# Beware : SLightly long run time %sql SELECT COUNT(*) FROM Clicks

# COMMAND ----------

## You can do ```%sql DESCRIBE Clicks or SHOW CREATE TABLE Clicks

# COMMAND ----------

# MAGIC %sql SHOW CREATE TABLE Clicks

# COMMAND ----------

# MAGIC %sql DESCRIBE Clicks

# COMMAND ----------

# Won't work for external tables %sql ALTER TABLE Clicks CHANGE V0 SessionID INT

# COMMAND ----------

# MAGIC %sql SELECT * FROM Clicks ORDER BY V0 LIMIT 10

# COMMAND ----------

# MAGIC %sql SELECT COUNT(*) FROM clicks -- takes 148s

# COMMAND ----------

# MAGIC %md ####1. Session ID ? ID of the session. In one session there are one or many clicks. Could be represented as an integer number.
# MAGIC ####2. Timestamp ? Time when the click occurred. Format of YYYY-MM-DDThh:mm:ss.SSSZ
# MAGIC ####3. Item ID ? Unique identifier of the item that has been clicked. Could be represented as an integer number.
# MAGIC ####4. Category ? Context of the click.
# MAGIC #####"S" indicates a special offer,
# MAGIC #####"0" indicates  a missing value,
# MAGIC #####A number between 1 to 12 indicates a real category identifier
# MAGIC #####Any other number indicates a brand. E.g. if an item has been clicked in the context of a promotion or special offer then the value will be "S", if the context was a brand i.e BOSCH,then the value will be an 8-10 digits number. If the item has been clicked under regular category, i.e. sport, then the value will be a number between 1 to 12. 

# COMMAND ----------

# MAGIC %sql CREATE TABLE Buys
# MAGIC USING com.databricks.spark.csv
# MAGIC OPTIONS (path "/mnt/data/recSys-2015/yoochoose-buys.dat", header "false")

# COMMAND ----------

# MAGIC %sql SELECT COUNT(*) FROM Buys

# COMMAND ----------

# MAGIC %sql SELECT * FROM Buys ORDER BY V0 LIMIT 10

# COMMAND ----------

# MAGIC %md ####1. Session ID - Id of the session. In one session there are one or many buying events. Could be represented as an integer number.
# MAGIC ####2. Timestamp - Time when the buy occurred. Format of YYYY-MM-DDThh:mm:ss.SSSZ
# MAGIC ####3. Item ID ? the unique identifier of item that has been bought. Could be represented as an integer number.
# MAGIC ####4. Price ? the price of the item. Could be represented as an integer number.
# MAGIC ####5. Quantity ? the quantity in this buying.  Could be represented as an integer number.```

# COMMAND ----------

# MAGIC %sql CREATE TABLE Test
# MAGIC USING com.databricks.spark.csv
# MAGIC OPTIONS (path "/mnt/data/recSys-2015/yoochoose-test.dat", header "false")

# COMMAND ----------

# MAGIC %sql SELECT * FROM Test LIMIT 10

# COMMAND ----------

# MAGIC %md ##### Test same as Click ie Session ID,Time Stamp,Item Id, Click Context

# COMMAND ----------

# MAGIC %md ####The task is to predict for each session in the test file, whether there is going to be a buying event in this session, and if there is, what are the items that will be bought. _No need to predict quantities._
# MAGIC ####The solution file, that has to be submitted, comprises records that have exactly two fields:
# MAGIC ####solution.dat
# MAGIC ####1. Session ID
# MAGIC ####2. Comma separated list of Item IDs that have been bought in this session

# COMMAND ----------

# MAGIC %md 
# MAGIC 2263615;214748295
# MAGIC 
# MAGIC 2541095;214748291,214748300,214831965,214831948

# COMMAND ----------

# MAGIC %md Items Per category

# COMMAND ----------

# MAGIC %sql SELECT V3 AS Category,COUNT(V3) As NoOfItems FROM Clicks GROUP By V3 ORDER BY NoOfItems DESC LIMIT 15

# COMMAND ----------

# MAGIC %sql SELECT COUNT(DISTINCT V2) FROM Clicks -- Unique Items in Clicks

# COMMAND ----------

# MAGIC %sql SELECT COUNT(DISTINCT V2) FROM Buys -- Unique Items in Clicks

# COMMAND ----------

# MAGIC %sql SELECT COUNT(DISTINCT V2) FROM Test -- Unique Items in Test

# COMMAND ----------

# MAGIC %sql SELECT V2 AS Item,COUNT(V2) As CountClicked, V3 as Category FROM Clicks GROUP By V2,V3 ORDER BY CountClicked DESC LIMIT 15 -- Top Items Clicked

# COMMAND ----------

# MAGIC %sql SELECT V2 AS Item,COUNT(V2) As Bought FROM Buys GROUP By V2 ORDER BY Bought DESC LIMIT 15 --Top Items bought
# MAGIC --Top Items bought by qty

# COMMAND ----------

# MAGIC %sql SELECT V2 AS Item,COUNT(V2) As Bought, SUM(V4) AS Qty, SUM(V3) AS TotalPrice FROM Buys GROUP By V2 ORDER BY Bought DESC LIMIT 15 --Top Items bought by qty

# COMMAND ----------

# The 0 is missing data. Let us see how many

# COMMAND ----------

# MAGIC %sql SELECT COUNT(*) FROM Buys

# COMMAND ----------

# MAGIC %sql SELECT COUNT(*) FROM Buys WHERE V4 < 1

# COMMAND ----------

610030/1150753.0

# COMMAND ----------

# MAGIC %sql SELECT V2 AS Item,COUNT(V2) As Bought, SUM(V4) AS Qty, SUM(V3) AS TotalPrice FROM Buys GROUP By V2 ORDER BY Qty LIMIT 100 --Items with 0 qty but bought. means No data

# COMMAND ----------

# MAGIC %sql SELECT V2 AS Test_Item,COUNT(V2) As Test_NoOfItems FROM Test GROUP By V2 ORDER BY Test_NoOfItems DESC LIMIT 15 --Top Items in Test Set

# COMMAND ----------

# Plan & Ideas - Stare at data/read Blogs/Think/Extrapolate experience
#
# Assumptions
#   Assume Session ID are independnt ie not a user id
#   Session ID is relevant, but needs feature engineering
#   Are Items independent of each other (ie i.i.d) - is there a relationship between clicks of multiple items and a buy ?
# To Try
#   Do all sessions in buy have respective clicks ? <Yes>
#   Clicked - Bought ratio by Category, by Item
#   Category - Bought ratio by Category, by Item
#   Items not in Clicked but in Test ? What would we do ? category base default ? Time based default ? Or ensamble ?
#   Time - Ignored here
#     Time dimension (HW) - which category, item has the best chance of buying based on time of click
#     Time Period in Daypart ? Hour granularity ? 30 min granularity ?
#     Browse time of an item vs Buy
#     Browse only items - never bought
#     Browse Time vs buy conversion ratio
#     Number of times browser vs buy
#     Session length
#     Session Clicks
#     Buy efficiency
#     Run STL - weekly trends - What is the seasonality ? Hour of week ?
#     Run ARIMA
#     Aggregate Day of the week - Buys vs Clicks, Histogram 
#     Aggregate by DayPart of week 24 X 7 = 168 - Buy vs. Click
#
# Inferences
# 
# 53 % Buys have no qty !
#
#


# COMMAND ----------

# MAGIC %md #### Would naive Bayes work ?
# MAGIC #### This is a Classicication Problem. But what are the features ?

# COMMAND ----------

# MAGIC %md ### RDD land !

# COMMAND ----------

from pyspark.sql import *
from dateutil.parser import *
clicks_rdd = sqlContext.table("Clicks").map(lambda row: (int(row[0]),parse(row[1]),int(row[2]),row[3]))

# COMMAND ----------

# beware : Takes ~4 minutes (250s)
clicks_rdd.count()

# COMMAND ----------

clicks_rdd.take(5)

# COMMAND ----------

from dateutil.parser import *
buys_rdd = sqlContext.table("Buys").map(lambda row: (int(row[0]),parse(row[1]),int(row[2]),int(row[3]),int(row[4])))

# COMMAND ----------

buys_rdd.count()

# COMMAND ----------

buys_rdd.take(5)

# COMMAND ----------

# Careful : Takes a few minutes. Cancelled after 666s
clicks_rdd.sortBy(lambda x: x[1],ascending=False).take(10)

# COMMAND ----------

# Do all buys have sessions in the click rdd
# Probably an anti join
# May be substract by key would work
buys_kv = buys_rdd.map(lambda x: (x[0],x[2])) # return session, item id
clicks_kv = clicks_rdd.map(lambda x: (x[0],x[2]))


# COMMAND ----------

buys_kv.take(3)

# COMMAND ----------

clicks_kv.take(3)

# COMMAND ----------

# Careful : Takes ~ 7 minutes (413s) 
clicks_kv.subtractByKey(buys_kv).count() # Clicks without buys = 29,698,257 out of 33,003,944. 1,150,753 Buys

# COMMAND ----------

# Careful : Takes ~6.5 minutes (400s)
buys_kv.subtractByKey(clicks_kv).count() # Buys without clicks - none!

# COMMAND ----------

# Add Hour and Day of week to buys
import datetime
buys_dayPart = buys_rdd.map(lambda x : (int(x[0]),x[1].hour,int(x[1].strftime('%w')),int(x[2]))) # Forget about price & qty

# COMMAND ----------

# MAGIC %sql SELECT * FROM Buys LIMIT 3

# COMMAND ----------

buys_dayPart.take(3)

# COMMAND ----------

from pyspark.sql import *
schema = StructType([ StructField("SessionId", IntegerType(), False),
                     StructField("HourOfDay", IntegerType(),False),
                     StructField("DayOfWeek", IntegerType(),False),
                     StructField("ItemNumber", IntegerType(),False)])
srdd = sqlContext.applySchema(buys_dayPart, schema)
sqlContext.registerRDDAsTable(srdd, "BuysDayPart")

# COMMAND ----------

# MAGIC %sql SELECT * FROM BuysDayPart LIMIT 3

# COMMAND ----------

# MAGIC %sql SELECT HourOfDay as HourOfDay, COUNT(HourOfDay) As Total FROM BuysDayPart GROUP By HourOfDay -- takes ~2 min (127s)

# COMMAND ----------

# MAGIC %sql SELECT DayOfWeek as DayOfWeek, COUNT(DayOfWeek) As Total FROM BuysDayPart GROUP By DayOfWeek -- takes ~2 min (127s)

# COMMAND ----------

# Add Hour and Day of week to clicks
import datetime
clicks_dayPart = clicks_rdd.map(lambda x : (int(x[0]),x[1].hour,int(x[1].strftime('%w')),int(x[2]),x[3]))

# COMMAND ----------

clicks_dayPart.take(3)

# COMMAND ----------

# MAGIC %sql SELECT * FROM Clicks LIMIT 3

# COMMAND ----------

from pyspark.sql import *
schema = StructType([ StructField("SessionId", IntegerType(), False),
                     StructField("HourOfDay", IntegerType(),False),
                     StructField("DayOfWeek", IntegerType(),False),
                     StructField("ItemNumber", IntegerType(),False),
                     StructField("Category", StringType(),False)])
srdd = sqlContext.applySchema(clicks_dayPart, schema)
sqlContext.registerRDDAsTable(srdd, "ClicksDayPart")

# COMMAND ----------

# MAGIC %sql SELECT * FROm ClicksDayPart LIMIT 5

# COMMAND ----------

# MAGIC %sql SELECT HourOfDay as HourOfDay, COUNT(HourOfDay) As Total FROM ClicksDayPart GROUP By HourOfDay -- takes ~ 1 hr (3614s)

# COMMAND ----------

# MAGIC %sql SELECT DayOfWeek as DayOfWeek, COUNT(DayOfWeek) As Total FROM ClicksDayPart GROUP By DayOfWeek -- takes ~ 1 hr (3950s)
