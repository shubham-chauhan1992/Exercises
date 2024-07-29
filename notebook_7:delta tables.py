# Databricks notebook source
# MAGIC %sql
# MAGIC create table edsmanaged.bronze.customer(index int ,customer_id string,first_name string,last_name string,company string,city string,country string,phone_1 string,phone_2 string,email string,subscription_date string,website string) USING DELTA
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history edsmanaged.bronze.customer

# COMMAND ----------

# MAGIC %sql
# MAGIC describe edsmanaged.bronze.customer

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended edsmanaged.bronze.customer

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into edsmanaged.bronze.customer values (1,"832B1AAabBCaA1BC","shubham","chauhan","exusia","chicago","United States","+1-224-248-2770","+91-7757009320","shubham.wip123@gmail.com","2024-05-31","https://www.shubham_chauhan.com/")

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe history edsmanaged.bronze.customer

# COMMAND ----------

from pyspark.sql.types import StructType,StringType,DateType,IntegerType,StructField
# Ans 2 
csv_file_schema=StructType([StructField('index', IntegerType(), True), StructField('customer_id', StringType(), True), StructField('first_name', StringType(), True), StructField('last_name', StringType(), True), StructField('company', StringType(), True), StructField('city', StringType(), True), StructField('country', StringType(), True), StructField('phone_1', StringType(), True), StructField('phone_2', StringType(), True), StructField('email', StringType(), True), StructField('subscription_date', StringType(), True), StructField('website', StringType(), True)])

custDf=spark.read.format("csv").options(header="true",inferSchema="true").schema(csv_file_schema).load("/Volumes/edsingestion_ext_src/landingzone/source/landingzone/customer/*")

custDf.printSchema()
custDf.write.format("delta").mode("append").saveAsTable("edsmanaged.bronze.customer")




# COMMAND ----------

custDf.write.insertInto("edsmanaged.bronze.customer",overwrite=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select *from edsmanaged.bronze.customer
