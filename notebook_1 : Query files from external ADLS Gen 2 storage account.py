# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Exercise 1:
# MAGIC
# MAGIC 1. create an azure databricks cluster.
# MAGIC 2. create a Metastore to attach unity catalog to a workspace.
# MAGIC 3. enable metastore for the workspace.
# MAGIC 4. create a group (manage_metastore) by using the user management option.
# MAGIC 5. assign your user to that group.
# MAGIC 6. Now go to the metastore that you created and set the admin as the ( manage_metastore ) group.
# MAGIC
# MAGIC # Exercise 2:
# MAGIC
# MAGIC
# MAGIC 1. create a storage account in azure cloud .
# MAGIC 2. create a container and a subfolder and place your file in this subfolder .
# MAGIC 3. create a new catalog in the metastore
# MAGIC 4. create a storage credential for this external storage account in databricks.
# MAGIC 5. create an external location using the storage credential mentioned above in databricks.
# MAGIC 6. create a new catalog in the metastore 
# MAGIC 7. create a new schema in this new catalog .
# MAGIC 8. create a new external location inside this new schema .( the external location has to be an ADLS gen 2 account)
# MAGIC
# MAGIC # Exercise 3:
# MAGIC
# MAGIC 1. read the csv file present inside this newly added external location via sql query 
# MAGIC 2. read the csv file present inside this newly added external location via a python dataframe
# MAGIC 3. read the json file present inside this newly added external location via sql query 
# MAGIC 4. read the json file present inside this newly added external location via a python dataframe
# MAGIC 5. read the xml file present inside this newly added external location via sql query 
# MAGIC 6. read the xml file present inside this newly added external location via a python dataframe
# MAGIC 3. read the text file present inside this newly added external location via sql query 
# MAGIC 4. read the text file present inside this newly added external location via a python dataframe
# MAGIC 5. read the file present inside this newly added external location via sql query in binary format
# MAGIC 6. read the file present inside this newly added external location via a python dataframe in binary format
# MAGIC 5. read the file present inside this newly added external location via sql query in text format
# MAGIC 6. read the file present inside this newly added external location via a python dataframe in text format
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from csv.`/Volumes/eds_external_location/lob/ingestion`

# COMMAND ----------

df=spark.read.format("csv").options(header="true",inferSchema="true").load("/Volumes/eds_external_location/lob/ingestion")
display(df)

# COMMAND ----------

df=spark.sql("""select *from csv.`/Volumes/eds_external_location/lob/ingestion`""").
display(df)
