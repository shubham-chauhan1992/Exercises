# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 1:
# MAGIC 1. List the cataog created in notebook 1 using sql query. 
# MAGIC 2. create a schema name external_ingestion_sources in the catalog that you created in notebook1 using sql query.
# MAGIC 3. create a table via an sql query that uses the files present in the external location created in notebook 1 as source data
# MAGIC 4. Describe the metadata of the created table 
# MAGIC
# MAGIC NOTE: if you have mounted a cloud storage location as a volume under a schema in a ctalog  then the same cloud storage location cannot be as a location of the external tables

# COMMAND ----------

# MAGIC %sql
# MAGIC show catalogs;
# MAGIC use catalog eds_external_ingestion_source;
# MAGIC show schemas;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE external_ingestion_sources.lobname_external.customer 
# MAGIC USING CSV 
# MAGIC OPTIONS(header="True", delimiter=",", inferSchema="True")
# MAGIC LOCATION "abfss://lobname@edsingestionexternal.dfs.core.windows.net/source/customer/"
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table extended  external_ingestion_sources.lobname_external.customer
