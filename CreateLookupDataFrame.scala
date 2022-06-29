// Databricks notebook source
// MAGIC %scala
// MAGIC val fields =  dbutils.widgets.get("fields")
// MAGIC val sourceFile =  dbutils.widgets.get("source_file")
// MAGIC val targetTable =  dbutils.widgets.get("target_table")

// COMMAND ----------

// MAGIC %scala
// MAGIC spark.sql("drop table if exists jedi_lookups." + targetTable)

// COMMAND ----------

// MAGIC %scala
// MAGIC var success = 0
// MAGIC var errMEssage =""
// MAGIC var hasOverlaps:Long= 0
// MAGIC var numRecords:Long = 0
// MAGIC 
// MAGIC try {
// MAGIC 
// MAGIC   val sql = """
// MAGIC     CREATE EXTERNAL TABLE IF NOT EXISTS jedi_lookups.""" + targetTable  + """(""" + fields + """)
// MAGIC       ROW FORMAT DELIMITED
// MAGIC       FIELDS TERMINATED BY '|'
// MAGIC       STORED AS TEXTFILE
// MAGIC       location 'dbfs:/mnt/trainingDatabricks/""" + sourceFile + """'"""
// MAGIC 
// MAGIC   spark.sql(sql)
// MAGIC   val df = spark.sql("select count(*) from jedi_lookups."+  targetTable)
// MAGIC   numRecords =df.take(1)(0)(0).asInstanceOf[Number].longValue
// MAGIC   success=1
// MAGIC 
// MAGIC }
// MAGIC catch {
// MAGIC   
// MAGIC   case ex : Throwable =>  errMEssage = ex.toString()
// MAGIC                        
// MAGIC }
// MAGIC 
// MAGIC finally {
// MAGIC   dbutils.notebook.exit("""{
// MAGIC         "cmd": 3,
// MAGIC         "status":""" +   success + """,
// MAGIC         "error":""""  + errMEssage + """",
// MAGIC         "num_records":"""  + numRecords + """,
// MAGIC         "has_overlaps":"""  + hasOverlaps + """        
// MAGIC     }"""
// MAGIC   )
// MAGIC   
// MAGIC }

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from jedi_lookups.L_LandCoverFlowCarbonS

// COMMAND ----------


