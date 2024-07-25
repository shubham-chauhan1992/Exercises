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
# MAGIC 8. use group by operation and apply following aggregaion functions min,max,count,count_if,sum,last,first,every,collect_list,collect_set,avg,any
# MAGIC 9. Normalize 1D array 

# COMMAND ----------

filename="/Volumes/edsingestion_ext_src/landingzone/source/landingzone/customer/*"
fileDf=spark.read.format("csv").options(header="true",inferSchema="true").load(filename)

# COMMAND ----------

from pyspark.sql.functions import *
# Ans 1. Filter Data
filterData=spark.read.format("csv").options(header="true",inferSchema="true").load(filename).filter(col("Index")%2==0)
display(filterData)

# COMMAND ----------

from pyspark.sql.functions import *
# Ans 2. Drop any row that has Null Values in any column
filterData=spark.read.format("csv").options(header="true",inferSchema="true").load(filename)
addNullValue=filterData.withColumn("addNullCol",when(col("index")%2==0,lit(None)).otherwise("keep"))
display(addNullValue)
dropNullValue=addNullValue.dropna()
display(dropNullValue)

# COMMAND ----------

from pyspark.sql.functions import *
# Ans 3. check blank value 
filterData=spark.read.format("csv").options(header="true",inferSchema="true").load(filename)
checkNullValueFlag=filterData.withColumn("addNullCol",when(col("index")%2==0,lit(None)).otherwise("keep")).withColumn("NullFlag",when(isnull(col("addNullCol")),"Yes").otherwise("No"))
display(checkNullValueFlag)


# COMMAND ----------

from pyspark.sql.functions import *
# Ans 4. check blank value 
filterData=spark.read.format("csv").options(header="true",inferSchema="true").load(filename)
checkBlankValueFlag=filterData.withColumn("addBlankCol",when(col("index")%2==0,lit("")).otherwise("keep")).withColumn("BlankFlag",when((col("addBlankCol")==""),"Yes").otherwise("No"))
display(checkBlankValueFlag)

# COMMAND ----------

from pyspark.sql.functions import *
# Ans 5. Select specific Columns from a Dataframe
selectDf=fileDf.select("Index","Customer Id","First Name","Last Name","Company")
display(selectDf)


# COMMAND ----------

from pyspark.sql.functions import *
# Ans 6. Drop specific Columns from a Dataframe
fileDf=spark.read.format("csv").options(header="true",inferSchema="true").load(filename)
dropColumnDf=fileDf.drop("Index","Customer Id","First Name","Last Name","Company")
display(dropColumnDf)

# COMMAND ----------

from pyspark.sql.functions import *
# Ans 7.Sort data based on several Columns from a Dataframe
fileDf=spark.read.format("csv").options(header="true",inferSchema="true").load(filename)
sortAscDf=fileDf.orderBy(["Country","City"],ascending=True)
display(sortAscDf)
sortDescDf=fileDf.orderBy(["Country","City"],ascending=False)
display(sortDescDf)

# COMMAND ----------

from pyspark.sql.functions import *
# Ans 8a. Use count() and count_if()function  Columns from a Dataframe
fileDf=spark.read.format("csv").options(header="true",inferSchema="true").load(filename)
groupByDf=fileDf.groupBy(concat(trim(col("Country"))).alias("country")).agg(count(trim(col("Country")))).orderBy(trim(col("Country")))
display(groupByDf)
countIfDf=fileDf.groupBy(trim(col("Country")).alias("country")).agg(count_if(col("index")%2==0).alias("conditionalCount")).orderBy("country")
display(countIfDf)


# COMMAND ----------

from pyspark.sql.functions import *
# Ans 8b. Use max() and min() function  Columns from a Dataframe
groupByMaxDf=fileDf.groupBy(concat(trim(col("Country"))).alias("country")).agg(max(concat(trim(col("Country")),lit("_"),trim(col("City")))).alias("countryCityCombinationMax"))
display(groupByMaxDf)
groupByMinDf=fileDf.groupBy(concat(trim(col("Country"))).alias("country")).agg(min(concat(trim(col("Country")),lit("_"),trim(col("City")))).alias("countryCityCombinationMin"))
display(groupByMinDf)

# COMMAND ----------

from pyspark.sql.functions import *
# Ans 8c. Use sum() and avg() function  Columns from a Dataframe
groupBySumDf=fileDf.groupBy(concat(trim(col("Country"))).alias("country")).agg(sum(col("Index"))).alias("sum_index_by_country")
display(groupBySumDf)
groupByAvgDf=fileDf.groupBy(concat(trim(col("Country"))).alias("country")).agg(avg(col("Index")).alias("avg_index_by_country"),count(col("Index")).alias("count_of_index"))
display(groupByAvgDf)

# COMMAND ----------

from pyspark.sql.functions import *
# Ans 8d. Use collect_set and collect_list() function  Columns from a Dataframe
collectSetDf=fileDf.groupBy(concat(trim(col("Country"))).alias("country")).agg(collect_set(col("country")).alias("countryset"))
display(collectSetDf)
collectListDf=fileDf.groupBy(concat(trim(col("Country"))).alias("country")).agg(collect_list(col("country")).alias("countrylist"))
display(collectListDf)

# COMMAND ----------

from pyspark.sql.functions import *
# Ans 8e. Use first() and last() function  Columns from a Dataframe

firstDf = fileDf.orderBy("country","index").groupBy(concat(trim(col("Country"))).alias("country")).agg(first(col("Index")).alias("first_index")).orderBy("country")
display(fileDf.orderBy("Country","Index"))
display(firstDf)

lastDf = fileDf.orderBy("country","index").groupBy(concat(trim(col("Country"))).alias("country")).agg(last(col("Index")).alias("last_index")).orderBy("country")

display(fileDf.orderBy(col("Country").asc(),col("Index").desc()))
display(lastDf)


# COMMAND ----------

# ans 9 Normalize a 1D array
from pyspark.sql.functions import *
# Ans 7d. Use collect_set and collect_list() function  Columns from a Dataframe


#craete an array first
arrayDf=fileDf.groupBy(concat(trim(col("Country"))).alias("country")).agg(collect_list(col("city")).alias("cityarray"),(size(collect_list(col("city")))).alias("sizeOfCityArray"))
display(arrayDf)

#Question : What if once we have done the grouping we want other column values also along with the aggregate columns.

#normalize an array 
normDf=arrayDf.withColumn("Normalized_countries",explode(col("cityarray"))).drop("cityarray")
display(normDf)

