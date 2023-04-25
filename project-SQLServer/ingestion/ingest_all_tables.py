# Databricks notebook source
# MAGIC %run "../../project-SQLServer/configuration/connection"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ingest Person.Person table

# COMMAND ----------

person_df = spark.read.option('header', True)\
.jdbc(url=jdbcUrl, table='Person.Person', properties=connectionProperties)\
.select('BusinessEntityID', "FirstName", "MiddleName","LastName")

display(person_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ingest Person.Address table

# COMMAND ----------

address_df = spark.read.option('header', True)\
.jdbc(url=jdbcUrl, table='Person.Address', properties=connectionProperties)\
.withColumnRenamed('AddressLine1', 'AddressLine')\
.select('AddressID', 'StateProvinceID', 'AddressLine', 'City', 'PostalCode')

display(address_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ingest Person.PersonPhone table

# COMMAND ----------

person_phone_df = spark.read.option('header', True).jdbc(url=jdbcUrl, table='Person.PersonPhone', properties=connectionProperties)\
.select('BusinessEntityID', 'PhoneNumber')

display(person_phone_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ingest Person.EmailAddress table

# COMMAND ----------

person_email_df = spark.read.option('header', True).jdbc(url=jdbcUrl, table='Person.EmailAddress', properties=connectionProperties)\
.select('BusinessEntityID', 'EmailAddress')

display(person_email_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ingest Person.BusinessEntityAddress table

# COMMAND ----------

business_entity_address_df = spark.read.option('header', True).jdbc(url=jdbcUrl, table='Person.BusinessEntityAddress', properties=connectionProperties)\
.select('BusinessEntityID', 'AddressID')

display(business_entity_address_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ingest Person.CountryRegion table

# COMMAND ----------

person_country_region_df = spark.read.option('header', True).jdbc(url=jdbcUrl, table='Person.CountryRegion', properties=connectionProperties)\
.withColumnRenamed('Name', 'Country')

display(person_country_region_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ingest Person.StateProvince table

# COMMAND ----------

state_province_df = spark.read.option('header', True).jdbc(url=jdbcUrl, table='Person.StateProvince', properties=connectionProperties)\
.withColumnRenamed('Name', 'State')\
.select('StateProvinceID', 'CountryRegionCode', 'State')

display(state_province_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join tables to make complete_address

# COMMAND ----------

complete_address_df = address_df.join(business_entity_address_df, address_df.AddressID == business_entity_address_df.AddressID, 'inner')\
                                .join(state_province_df, address_df.StateProvinceID == state_province_df.StateProvinceID)\
                                .join(person_country_region_df, person_country_region_df.CountryRegionCode == state_province_df.CountryRegionCode)\
                                .select('BusinessEntityID', 'AddressLine', 'PostalCode', 'City', 'State', 'Country')
display(complete_address_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join tables to make complete_person

# COMMAND ----------

from pyspark.sql.functions import when, aggregate, col, concat, expr

complete_person_df = person_df.join(person_email_df, person_df.BusinessEntityID == person_email_df.BusinessEntityID)\
                              .join(person_phone_df, person_df.BusinessEntityID == person_phone_df.BusinessEntityID)\
                              .withColumn('Name', expr("CASE WHEN MiddleName != 'null' THEN CONCAT(FirstName, ' ', MiddleName, ' ', LastName) ELSE CONCAT(FirstName, ' ', LastName) END"))\
                              .select(person_df.BusinessEntityID, 'Name', 'EmailAddress', 'PhoneNumber')

complete_person_df = complete_person_df.dropDuplicates(subset=["BusinessEntityID"]).select('BusinessEntityID', 'Name', 'EmailAddress', 'PhoneNumber')

display(complete_person_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join tables to make final_person_contact

# COMMAND ----------

final_person_contact_df = complete_person_df.join(complete_address_df, complete_person_df.BusinessEntityID == complete_address_df.BusinessEntityID)\
                                            .select(complete_person_df.BusinessEntityID, 'Name', 'EmailAddress', 'PhoneNumber', 'AddressLine', 'PostalCode', 'City', 'State', 'Country')
display(final_person_contact_df)

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC /* Create the table in Azure SQL Database*/
# MAGIC 
# MAGIC CREATE TABLE Person.PersonContact (
# MAGIC BusinessEntityID INT PRIMARY KEY, 
# MAGIC Name NVARCHAR(50), 
# MAGIC EmailAddress NVARCHAR(50),
# MAGIC PhoneNumber NVARCHAR(30),
# MAGIC AddressLine NVARCHAR(50),
# MAGIC PostalCode NVARCHAR(15),
# MAGIC City NVARCHAR(20), 
# MAGIC State NVARCHAR(20),
# MAGIC Country VARCHAR(30)
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write the df into the created table

# COMMAND ----------

final_person_contact_df.write.format('jdbc')\
                             .mode('overwrite')\
                             .option("url", jdbcUrl)\
                             .option('port', jdbcPort)\
                             .option('user', jdbcUsername)\
                             .option('password',jdbcPassword)\
                             .option('database',jdbcDatabase)\
                             .option('dbtable', 'Person.PersonContact')\
                             .save()
