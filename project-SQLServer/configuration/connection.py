# Databricks notebook source
jdbcHostname = "sql-adventure-server.database.windows.net"
jdbcDatabase = "AdventureWorks-db"
jdbcPort = 1433
jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase}"
jdbcUsername = dbutils.secrets.get(scope = 'jdbc-scope', key='jdbcUsername')
jdbcPassword = dbutils.secrets.get(scope = 'jdbc-scope', key='jdbcPassword')

connectionProperties = {
  "user": jdbcUsername,
  "password": jdbcPassword,
  "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("AzureSQL").getOrCreate()

# set Azure SQL Database properties in Spark config
spark.conf.set("spark.sql.azuresqldb.serverName", jdbcHostname)
spark.conf.set("spark.sql.azuresqldb.databaseName", jdbcDatabase)
spark.conf.set("spark.sql.azuresqldb.user", jdbcUsername)
spark.conf.set("spark.sql.azuresqldb.password", jdbcPassword)


# COMMAND ----------



