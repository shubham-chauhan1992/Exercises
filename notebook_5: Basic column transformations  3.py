# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise :
# MAGIC 1. Filter data based on a condition
# MAGIC 2. drop records that have null value
# MAGIC 3. check is null condition
# MAGIC 4. check is blank condition
# MAGIC 5. select only a required columns.
# MAGIC 6. drop unwanted columns.
# MAGIC 7. sort the data based on a key
# MAGIC 8. use group by operation and apply following aggregaion functions min,max,count,count_if,sum,last,first,every,collect_list,collect_set,array_agg,avg,any
# MAGIC 9. Normalize 1D array 
# MAGIC 10. Normalize a 2D array

# COMMAND ----------

from pyspark.sql.functions import *
# Ans 1. Filter Data
filterData=spark.read.format("csv").options(header="true",inferSchema="true").load("/Volumes/edsingestion_ext_src/landingzone/source/customer/*").filter(col("Index")%2==0)
display(filterData)

# COMMAND ----------

from pyspark.sql.functions import *
# Ans 2. Drop any row that has Null Values in any column
filterData=spark.read.format("csv").options(header="true",inferSchema="true").load("/Volumes/edsingestion_ext_src/landingzone/source/customer/*")
addNullValue=filterData.withColumn("addNullCol",when(col("index")%2==0,lit(None)).otherwise("keep"))
display(addNullValue)
dropNullValue=addNullValue.dropna()
display(dropNullValue)

# COMMAND ----------

from pyspark.sql.functions import *
# Ans 3. check blank value 
filterData=spark.read.format("csv").options(header="true",inferSchema="true").load("/Volumes/edsingestion_ext_src/landingzone/source/customer/*")
checkNullValueFlag=filterData.withColumn("addNullCol",when(col("index")%2==0,lit(None)).otherwise("keep")).withColumn("NullFlag",when(isnull(col("addNullCol")),"Yes").otherwise("No"))
display(checkNullValueFlag)


# COMMAND ----------

from pyspark.sql.functions import *
# Ans 4. check blank value 
filterData=spark.read.format("csv").options(header="true",inferSchema="true").load("/Volumes/edsingestion_ext_src/landingzone/source/customer/*")
checkBlankValueFlag=filterData.withColumn("addBlankCol",when(col("index")%2==0,lit("")).otherwise("keep")).withColumn("BlankFlag",when((col("addBlankCol")==""),"Yes").otherwise("No"))
display(checkBlankValueFlag)

# COMMAND ----------

from pyspark.sql.functions import *
# Ans 5. Select specific Columns from a Dataframe
fileDf=spark.read.format("csv").options(header="true",inferSchema="true").load("/Volumes/edsingestion_ext_src/landingzone/source/customer/*")
selectDf=fileDf.select("Index","Customer Id","First Name","Last Name","Company")
display(selectDf)


# COMMAND ----------

from pyspark.sql.functions import *
# Ans 6. Drop specific Columns from a Dataframe
fileDf=spark.read.format("csv").options(header="true",inferSchema="true").load("/Volumes/edsingestion_ext_src/landingzone/source/customer/*")
dropColumnDf=fileDf.drop("Index","Customer Id","First Name","Last Name","Company")
display(dropColumnDf)

# COMMAND ----------

from pyspark.sql.functions import *
# Ans 6.Sort data based on several Columns from a Dataframe
fileDf=spark.read.format("csv").options(header="true",inferSchema="true").load("/Volumes/edsingestion_ext_src/landingzone/source/customer/*")
sortAscDf=fileDf.orderBy(["Country","City"],ascending=True)
display(sortAscDf)
sortDescDf=fileDf.orderBy(["Country","City"],ascending=False)
display(sortDescDf)

# COMMAND ----------

from pyspark.sql.functions import *
# Ans 7a. Use count() function  Columns from a Dataframe
fileDf=spark.read.format("csv").options(header="true",inferSchema="true").load("/Volumes/edsingestion_ext_src/landingzone/source/customer/*")
groupByDf=fileDf.groupBy(concat(trim(col("Country")),lit("_"),trim(col("City"))).alias("country_city")).agg(count(concat(trim(col("Country")),lit("_"),trim(col("City")))).alias("countryCityCombinationCount")).orderBy(concat(trim(col("Country")),lit("_"),trim(col("City")))).filter(col("countryCityCombinationCount")>1)
display(groupByDf)


# COMMAND ----------

from pyspark.sql.functions import *
# Ans 7b. Use max() and min() function  Columns from a Dataframe
fileDf=spark.read.format("csv").options(header="true",inferSchema="true").load("/Volumes/edsingestion_ext_src/landingzone/source/customer/*")
groupByMaxDf=fileDf.groupBy(concat(trim(col("Country"))).alias("country")).agg(max(concat(trim(col("Country")),lit("_"),trim(col("City")))).alias("countryCityCombinationMax"))
display(groupByMaxDf)
groupByMinDf=fileDf.groupBy(concat(trim(col("Country"))).alias("country")).agg(min(concat(trim(col("Country")),lit("_"),trim(col("City")))).alias("countryCityCombinationMin"))
display(groupByMinDf)

# COMMAND ----------

from pyspark.sql.functions import *
# Ans 7c. Use sum() and avg() function  Columns from a Dataframe
fileDf=spark.read.format("csv").options(header="true",inferSchema="true").load("/Volumes/edsingestion_ext_src/landingzone/source/customer/*")
groupBySumDf=fileDf.groupBy(concat(trim(col("Country"))).alias("country")).agg(sum(col("Index"))).alias("sum_index_by_country")
display(groupBySumDf)
groupByAvgDf=fileDf.groupBy(concat(trim(col("Country"))).alias("country")).agg(avg(col("Index")).alias("avg_index_by_country"),count(col("Index")).alias("count_of_index"))
display(groupByAvgDf)

# COMMAND ----------

from pyspark.sql.functions import *
# Ans 7d. Use collect and avg() function  Columns from a Dataframe
fileDf=spark.read.format("csv").options(header="true",inferSchema="true").load("/Volumes/edsingestion_ext_src/landingzone/source/customer/*")
groupBySumDf=fileDf.groupBy(concat(trim(col("Country"))).alias("country")).agg(sum(col("Index"))).alias("sum_index_by_country")
display(groupBySumDf)
groupByAvgDf=fileDf.groupBy(concat(trim(col("Country"))).alias("country")).agg(avg(col("Index")).alias("avg_index_by_country"),count(col("Index")).alias("count_of_index"))
display(groupByAvgDf)
