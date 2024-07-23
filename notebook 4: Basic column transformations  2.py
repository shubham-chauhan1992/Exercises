# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise : 
# MAGIC 1. convert to lowercase.
# MAGIC 2. convert to uppercase.
# MAGIC 3. calculate the string length.
# MAGIC 4. extract a sub string based on position from a full string. 
# MAGIC 5. create a 1D array of a string that is delimited by a character and map it to a new column.
# MAGIC 6. use pattern matching to write a conditional statement.
# MAGIC 7. removing the spaces from the start and end of a string column.
# MAGIC 8. replace a character present inside a string with another string.
# MAGIC 9. check the starting character pattern of a string to write a conditional statement.
# MAGIC 10. check the ending character pattern of a string to write a conditional statement.
# MAGIC 11. concatenate a string .
# MAGIC 12. find the position of a character in a string and use it to extract a substing.
# MAGIC 13. Remove special charaters from a string.
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType,StringType,DateType,IntegerType,StructField

csv_file_schema=StructType([StructField('index', IntegerType(), True), StructField('customer_id', StringType(), True), StructField('first_name', StringType(), True), StructField('last_name', StringType(), True), StructField('company', StringType(), True), StructField('city', StringType(), True), StructField('country', StringType(), True), StructField('phone_1', StringType(), True), StructField('phone_2', StringType(), True), StructField('email', StringType(), True), StructField('subscription_date', DateType(), True), StructField('website', StringType(), True)])

csvDf=spark.read.format('csv').options(header="true").schema(csv_file_schema).load("/Volumes/eds_external_ingestion_source/landingzone/sources/landingzone/customer/*")
display(csvDf)

# COMMAND ----------

from pyspark.sql.functions import *

# Ans 1. Convert to lowercase
convertToLowerCaseDf=csvDf.select(col("city")).withColumn("newcity",lower(col("city")))
display(convertToLowerCaseDf)

# COMMAND ----------

from pyspark.sql.functions import *

# Ans 2. Convert to uppercase
convertToUpperCaseDf=csvDf.select(col("city")).withColumn("newcity",upper(col("city")))
display(convertToUpperCaseDf)

# COMMAND ----------

from pyspark.sql.functions import *

# Ans 3. Calculate length of a string
lenOfStringDf=csvDf.select(col("city")).withColumn("lenOfCity",length(col("city")))
display(lenOfStringDf)

# COMMAND ----------

from pyspark.sql.functions import *

# Ans 4. Extract a substring from an existing string
subStringDf=csvDf.select(col("city")).withColumn("subStrFromCity",substring(col("city"),1,4))
display(subStringDf)

# COMMAND ----------

from pyspark.sql.functions import *

# Ans 5. Create a 1D array of a string separated by a delimiter For eg "-"
subStringDf=csvDf.withColumn("emailarray",split(col("email"),"@"))
display(subStringDf)

# COMMAND ----------

# Ans 6. use pattern matching to write a conditional statement.
patternMatchDf=csvDf.withColumn("checkIfWebsitehas.OrgFlag",when(col("website").like(".Org"),"non-profit").otherwise("profit"))
display(patternMatchDf)

# COMMAND ----------

# Ans 7 removing the spaces from the start and end of a string column.
trimhleadingTrailingSpacesDf=csvDf.withColumn("addSpacesToWebsiteColumn",concat(lit(" "),col("website"),lit(""))).withColumn("trimWebsiteValue",trim(col("website"))).select("website","addSpacesToWebsiteColumn","trimWebsiteValue")
display(trimhleadingTrailingSpacesDf)

# COMMAND ----------

# Ans 8 replace a character present inside a string with another string.
replaceCharacterDf=csvDf.withColumn("phone1replace",replace(trim(col("phone_1")),lit("+1-"),lit("")))
display(replaceCharacterDf)

# COMMAND ----------


