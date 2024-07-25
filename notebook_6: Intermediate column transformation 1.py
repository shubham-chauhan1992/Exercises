# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise
# MAGIC 1. Use Window Functions
# MAGIC
# MAGIC

# COMMAND ----------

filename="/Volumes/edsingestion_ext_src/landingzone/source/landingzone/customer/*"
fileDf=spark.read.format("csv").options(header="true",inferSchema="true").load(filename)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import *

# Define a Window Specification
windowSpec = Window.partitionBy("Country").orderBy("Country")

# Row Number
rowNumWindowFuncDf = fileDf.withColumn("row_number", F.row_number().over(windowSpec)).where(trim(col("country"))=='Afghanistan')
display(rowNumWindowFuncDf)

#remove duplicates using row_number
windowSpec2 = Window.partitionBy("Country","Subscription Date").orderBy("Subscription Date")
removeDupOnKeyDf = fileDf.withColumn("row_number", F.row_number().over(windowSpec2)).filter( (trim(col("Country"))=='Afghanistan') ).orderBy("Subscription Date")
display(removeDupOnKeyDf)



# COMMAND ----------


from pyspark.sql import functions as F
from pyspark.sql.functions import *

salaryDf=spark.read.format("csv").options(header="true",inferSchema="true").load("/Volumes/edsingestion_ext_src/landingzone/source/landingzone/customer/customers_sal.csv")

#adding window spec
windowSpecRank = Window.partitionBy("Country").orderBy("salary")
rankWindowFuncDf = salaryDf.withColumn("rankSalary", F.rank().over(windowSpecRank)).filter(col("Country")=="Afghanistan")
display(rankWindowFuncDf)



# COMMAND ----------

# Dense Rank
windowSpecDenseRank = Window.partitionBy("Country").orderBy("salary")
denseRankWindowFuncDf = salaryDf.withColumn("dense_rank", F.dense_rank().over(windowSpecDenseRank)).filter(col("Country")=="Afghanistan")
display(denseRankWindowFuncDf)



# COMMAND ----------

# Percent Rank
windowSpecPercentRank = Window.partitionBy("Country").orderBy("salary")
percentRankWindowFuncDf = salaryDf.withColumn("percent_rank", F.percent_rank().over(windowSpecPercentRank))
display(percentRankWindowFuncDf)

# COMMAND ----------

# Cumulative Distribution
windowSpecCumlaDist = Window.partitionBy("Country").orderBy("salary")
cumlaDistWindowFuncDf = salaryDf.withColumn("cume_dist", F.cume_dist().over(windowSpecCumlaDist))
display(cumlaDistWindowFuncDf)

# COMMAND ----------

#A good example of implementing lead function is to show reducing balance in a credit card transaction
#amount remaining in credit card
# 2024-01-01 , 2000$
# 2024-01-02 , 1950$
# 2024-01-02 , 1800$

#Find the amount spent each day


# Lead
windowSpecLead = Window.partitionBy("Country").orderBy("salary")
leadWindowFuncDf = salaryDf.withColumn("lead", F.lead("salary", 10).over(windowSpecLead))  # Change 1 to your desired offset
display(leadWindowFuncDf)

# COMMAND ----------

# we have to check for every customer that on what date the current date transaction was greater than the previous day transaction.

# Lag
windowSpecLag = Window.partitionBy("Country").orderBy("salary")
lagWindowFuncDf = salaryDf.withColumn("lag", F.lag("salary", 1).over(windowSpecLag))  # Change 1 to your desired offset
display(lagWindowFuncDf)

# COMMAND ----------

# Moving Average
windowSpecMovAvg = Window.partitionBy("Country").orderBy("salary")
movAvgWindowFuncDf = salaryDf.withColumn("moving_avg", F.avg("Salary").over(windowSpecMovAvg.rowsBetween(-1, 1)))  # Change -1, 1 to your desired frame
display(movAvgWindowFuncDf)

# COMMAND ----------

# Cumulative Sum
windowSpecCumvSum = Window.partitionBy("Country").orderBy("salary")
cumSumWindowFuncDf = salaryDf.withColumn("cumulative_sum", F.sum("Salary").over(windowSpecCumvSum.rowsBetween(Window.unboundedPreceding, 0)))
display(cumSumWindowFuncDf)

