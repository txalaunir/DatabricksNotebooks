// Databricks notebook source
// DBTITLE 1,Read the values of the input parameters
// MAGIC %scala
// MAGIC val fields =  dbutils.widgets.get("fields")
// MAGIC val sourceFile =  dbutils.widgets.get("source_file")
// MAGIC val targetTable =  dbutils.widgets.get("target_table")
// MAGIC 
// MAGIC var success = 0
// MAGIC var status = 0
// MAGIC var errMEssage =""
// MAGIC var hasOverlaps:Long= 0
// MAGIC var numRecords:Long = 0

// COMMAND ----------

// MAGIC %scala
// MAGIC spark.sql("drop table if exists jedi_dimensions." + targetTable)

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC try {
// MAGIC   val sql = """
// MAGIC     CREATE EXTERNAL TABLE IF NOT EXISTS jedi_dimensions.""" + targetTable  + """(""" + fields + """)
// MAGIC       ROW FORMAT DELIMITED fields terminated by '|' 
// MAGIC       location 'dbfs:/mnt/trainingDatabricks/""" + sourceFile + """'"""
// MAGIC   spark.sql(sql)
// MAGIC   
// MAGIC   val df = spark.sql("select count(*) from jedi_dimensions."+  targetTable)
// MAGIC   numRecords =df.take(1)(0)(0).asInstanceOf[Number].longValue
// MAGIC 
// MAGIC   //has overlaps?
// MAGIC   val overlapsDF = spark.sql("SELECT SUM(TotalDups) as TotalDuplicates FROM (   SELECT count(*) as TotalDups   FROM jedi_dimensions." + targetTable  + "  GROUP BY GridNum,GridNum10Km HAVING count(*) > 1)")
// MAGIC   hasOverlaps = if (overlapsDF.take(1)(0)(0)==null) 0 else overlapsDF.take(1)(0)(0).asInstanceOf[Number].longValue
// MAGIC   success =1 
// MAGIC }
// MAGIC catch {  
// MAGIC   case ex : Throwable =>  {
// MAGIC     if (ex.getCause != None.orNull)
// MAGIC      errMEssage = ex.getCause.toString
// MAGIC    else
// MAGIC      errMEssage = ex.getMessage
// MAGIC   }       
// MAGIC }
// MAGIC finally  {
// MAGIC     dbutils.notebook.exit("""{
// MAGIC         "cmd": 4 ,
// MAGIC         "status":""" +   success + """,
// MAGIC         "error":""""  + errMEssage + """",
// MAGIC         "num_records":"""  + numRecords + """,
// MAGIC         "has_overlaps":"""  + hasOverlaps + """        
// MAGIC     }"""
// MAGIC   )
// MAGIC }

// COMMAND ----------


