// Databricks notebook source
// DBTITLE 1,Read file that contains the SQL statements and parse it
// MAGIC %scala
// MAGIC import scala.collection.mutable.WrappedArray
// MAGIC import org.apache.spark.storage.StorageLevel._
// MAGIC spark.conf.set("spark.sql.broadcastTimeout",  36000)
// MAGIC spark.conf.set("spark.sql.adaptive.enabled",  true)
// MAGIC spark.conf.set("spark.databricks.adaptive.autoOptimizeShuffle.enabled",true)
// MAGIC spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", true)
// MAGIC //spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")
// MAGIC                
// MAGIC var G_TableTSQL: WrappedArray[String] = WrappedArray.empty[String]
// MAGIC var G_HighToLow_TSQL: WrappedArray[String] = WrappedArray.empty[String]
// MAGIC var F_TableTSQLScale: WrappedArray[String] = WrappedArray.empty[String]
// MAGIC var F_TableTSQLScale_Part: WrappedArray[String] = WrappedArray.empty[String]
// MAGIC var F_TableTSQLHightToLow: WrappedArray[String] = WrappedArray.empty[String]
// MAGIC //var F_TableTSQL_A_Caches:String = ""
// MAGIC var F_TableTSQL_A_MODEs: WrappedArray[String] = WrappedArray.empty[String]
// MAGIC var F_TableTSQL_A_Parts: WrappedArray[String] = WrappedArray.empty[String]
// MAGIC var F_TableTSQL_A:String = ""
// MAGIC var F_TableTSQL:String = ""
// MAGIC var C_TableTSQL_MODEs: WrappedArray[String] = WrappedArray.empty[String]
// MAGIC var C_TableTSQL:String = ""
// MAGIC var Fields:String =""
// MAGIC var success = 0
// MAGIC var errMEssage =""
// MAGIC 
// MAGIC 
// MAGIC try {
// MAGIC   //Read SQL_Queries_Files as JSON
// MAGIC   val queryFile = dbutils.widgets.get("SQL_Queries_File")
// MAGIC   val pathToFile ="dbfs:/mnt/trainingDatabricks/" + queryFile 
// MAGIC   val jsonData = sqlContext.read.json(pathToFile)
// MAGIC   
// MAGIC   //Parse the fields of the JSON and extract the SQL statements
// MAGIC   G_TableTSQL = jsonData.select("G_TableTSQL").head()(0).asInstanceOf[WrappedArray[String]]
// MAGIC   G_HighToLow_TSQL = jsonData.select("G_HighToLow_TSQL").head()(0).asInstanceOf[WrappedArray[String]]
// MAGIC   F_TableTSQLScale = jsonData.select("F_TableTSQLScale").head()(0).asInstanceOf[WrappedArray[String]]
// MAGIC   F_TableTSQLScale_Part = jsonData.select("F_TableTSQLScale_Part").head()(0).asInstanceOf[WrappedArray[String]]  
// MAGIC   F_TableTSQLHightToLow = jsonData.select("F_TableTSQLHighToLow").head()(0).asInstanceOf[WrappedArray[String]]
// MAGIC   //F_TableTSQL_A_Caches = jsonData.select("F_TableTSQL_A_Caches").head()(0).asInstanceOf[String]
// MAGIC   F_TableTSQL_A_MODEs = jsonData.select("F_TableTSQL_A_Caches").head()(0).asInstanceOf[WrappedArray[String]]
// MAGIC   F_TableTSQL_A_Parts = jsonData.select("F_TableTSQL_A_Parts").head()(0).asInstanceOf[WrappedArray[String]]
// MAGIC   F_TableTSQL_A = jsonData.select("F_TableTSQL_A").head()(0).asInstanceOf[String]
// MAGIC   F_TableTSQL = jsonData.select("F_TableTSQL").head()(0).asInstanceOf[String]
// MAGIC   C_TableTSQL_MODEs = jsonData.select("C_TableTSQL_MODEs").head()(0).asInstanceOf[WrappedArray[String]]
// MAGIC   C_TableTSQL = jsonData.select("C_TableTSQL").head()(0).asInstanceOf[String]
// MAGIC   Fields = jsonData.select("Fields").head()(0).asInstanceOf[String]
// MAGIC   
// MAGIC }
// MAGIC catch {  
// MAGIC   case ex : Throwable =>  {
// MAGIC     if (ex.getCause != None.orNull)
// MAGIC       errMEssage = ex.getCause.toString
// MAGIC     else
// MAGIC       errMEssage = ex.getMessage
// MAGIC     dbutils.notebook.exit(    
// MAGIC       """
// MAGIC         {
// MAGIC           "cmd": 1,
// MAGIC           "status":0,
// MAGIC           "error":""""  + errMEssage + """"
// MAGIC     }
// MAGIC     """
// MAGIC     )            
// MAGIC     }       
// MAGIC }

// COMMAND ----------

// DBTITLE 1,Build temp G tables
import org.apache.spark.sql
import org.apache.spark.storage.StorageLevel._


try {
  G_TableTSQL.foreach (tbl => {
    var sqlDrop = tbl.split(";")(0)
    val tableName =sqlDrop.substring(sqlDrop.toLowerCase().indexOf("exists") + 6)     
    var sqlCreate= tbl.split(";")(1)
    spark.sql(sqlDrop) 
    println("creating=>" +tableName)
    
    val resGTableDF =  spark.sql(sqlCreate) //.repartition($"GridNum10km",$"GridNum")       
    //resGTableDF.persist(MEMORY_AND_DISK_SER)
    //resGTableDF.createOrReplaceTempView( tableName.replace("jedi_cubes.",""))     
    //println(resGTableDF.count)
    resGTableDF.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tableName)       
  })
}
catch {  
  case ex : Throwable =>  {
    if (ex.getCause != None.orNull)
      errMEssage = ex.getCause.toString
    else
      errMEssage = ex.getMessage
    dbutils.notebook.exit(    
      """
        {
          "cmd": 2,
          "status":0,
          "error":""""  + errMEssage + """"
    }
    """
    )            
    }       
}              



// COMMAND ----------

// DBTITLE 1,Build partial dataframes for each dim size, so the joins can be done
// MAGIC %scala
// MAGIC import org.apache.spark.sql
// MAGIC 
// MAGIC //try {
// MAGIC   F_TableTSQLScale_Part.foreach (tbl => {    
// MAGIC     var sqlDrop = tbl.split(";")(0) 
// MAGIC     if (!sqlDrop.isEmpty) {
// MAGIC       val tableName =sqlDrop.substring(sqlDrop.toLowerCase().indexOf("exists") + 6).trim()
// MAGIC       var sqlCreate= tbl.split(";")(1).toLowerCase()    
// MAGIC       try {
// MAGIC         spark.sql(sqlDrop)   
// MAGIC         //try to create the dataframe/table parts for the next step's big query   )        
// MAGIC         println("creating=>" +tableName)    
// MAGIC         val resFTableDF =  spark.sql(sqlCreate)
// MAGIC 
// MAGIC         resFTableDF.write.format("delta").mode("overwrite").saveAsTable(tableName) 
// MAGIC 
// MAGIC       }
// MAGIC       catch {  
// MAGIC         case ex : Throwable =>  {
// MAGIC             if (ex.getCause != None.orNull)
// MAGIC               errMEssage = ex.getCause.toString
// MAGIC             else
// MAGIC               errMEssage = ex.getMessage
// MAGIC                 dbutils.notebook.exit(    
// MAGIC                   """
// MAGIC                     {
// MAGIC                       "cmd": 3,
// MAGIC                       "status":0,
// MAGIC                       "error":""""  + errMEssage + """"
// MAGIC                 }
// MAGIC                 """
// MAGIC                 )            
// MAGIC            } 
// MAGIC       }
// MAGIC     }
// MAGIC   })

// COMMAND ----------

// DBTITLE 1,Build temp F_TableTSQLScale
// MAGIC %scala
// MAGIC import org.apache.spark.sql
// MAGIC //try {
// MAGIC   F_TableTSQLScale.foreach (tbl => {
// MAGIC     var sqlDrop = tbl.split(";")(0) 
// MAGIC     if (!sqlDrop.isEmpty) {
// MAGIC         val tableName =sqlDrop.substring(sqlDrop.toLowerCase().indexOf("exists") + 6).trim()
// MAGIC         var sqlCreate= tbl.split(";")(1)              
// MAGIC         try {          println(sqlDrop)
// MAGIC           
// MAGIC           if(sqlCreate.indexOf("RENAME") != -1)
// MAGIC           {          
// MAGIC             println("Renaming=>" +tableName)    
// MAGIC             /*
// MAGIC             val tables= sqlCreate.split("RENAME TO")
// MAGIC             val tbl1= tables(0).split("ALTER TABLE ")(1).trim()
// MAGIC             val tbl2= tables(1).trim()            
// MAGIC             
// MAGIC             
// MAGIC             spark.catalog.dropTempView(tbl2.replace("jedi_cubes.","").replace("`",""))
// MAGIC             val resFTableDF =  spark.sql("select * from " + tbl1) //.repartition($"GridNum10km",$"GridNum")                           
// MAGIC             resFTableDF.persist(MEMORY_AND_DISK_SER)
// MAGIC             resFTableDF.createOrReplaceTempView(tbl2.replace("jedi_cubes.","").replace("`",""))             
// MAGIC             println(resFTableDF.count)                                     
// MAGIC             
// MAGIC             //println("select * from " + tbl1)
// MAGIC             //println(tbl2.replace("jedi_cubes.","").replace("`",""))
// MAGIC             */
// MAGIC             spark.sql(sqlCreate)
// MAGIC             
// MAGIC           }
// MAGIC           else
// MAGIC           {
// MAGIC             spark.sql(sqlDrop)   
// MAGIC             println("creating=>" +tableName)    
// MAGIC 
// MAGIC             //sqlCreate = sqlCreate.toLowerCase().replace("jedi_cubes.`g_", "`g_" )            
// MAGIC             //sqlCreate = sqlCreate.replace("jedi_cubes.`F_", "`F_" )
// MAGIC             /*
// MAGIC             spark.catalog.dropTempView(tableName.replace("jedi_cubes.",""))
// MAGIC             val resFTableDF =  spark.sql(sqlCreate) //.repartition($"GridNum10km",$"GridNum")                           
// MAGIC             resFTableDF.persist(MEMORY_AND_DISK_SER)
// MAGIC             resFTableDF.createOrReplaceTempView( tableName.replace("jedi_cubes.",""))             
// MAGIC             println(resFTableDF.count)                        
// MAGIC             
// MAGIC             //spark.catalog.dropTempView(tableName.replace("jedi_cubes.","").replace("`","").replace("F_","G_"))                        
// MAGIC             */
// MAGIC             val resFTableDF =  spark.sql(sqlCreate) //.repartition($"GridNum10km",$"GridNum")                                       
// MAGIC             resFTableDF.write.format("delta").mode("overwrite").saveAsTable(tableName)                         
// MAGIC           }
// MAGIC           
// MAGIC           
// MAGIC         }
// MAGIC         catch {  
// MAGIC           case ex : Throwable =>  {
// MAGIC             if (ex.getCause != None.orNull)
// MAGIC               errMEssage = ex.getCause.toString
// MAGIC             else
// MAGIC               errMEssage = ex.getMessage
// MAGIC                 dbutils.notebook.exit(    
// MAGIC                   """
// MAGIC                     {
// MAGIC                       "cmd": 4,
// MAGIC                       "status":0,
// MAGIC                       "error":""""  + errMEssage + """"
// MAGIC                 }
// MAGIC                 """
// MAGIC                 )            
// MAGIC            } 
// MAGIC         }
// MAGIC       }
// MAGIC     
// MAGIC   })

// COMMAND ----------

// DBTITLE 1,Build temp G_HighToLow_TSQL
// MAGIC %scala
// MAGIC import org.apache.spark.sql
// MAGIC 
// MAGIC try {
// MAGIC   G_HighToLow_TSQL.foreach (tbl => {
// MAGIC     if (!tbl.isEmpty) {
// MAGIC       var sqlDrop = tbl.split(";")(0)
// MAGIC       val tableName =sqlDrop.substring(sqlDrop.toLowerCase().indexOf("exists") + 6)     
// MAGIC       var sqlCreate= tbl.split(";")(1).toLowerCase()
// MAGIC       spark.sql(sqlDrop) 
// MAGIC 
// MAGIC       val resGTableDF =  spark.sql(sqlCreate) 
// MAGIC       resGTableDF.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tableName)       
// MAGIC     }
// MAGIC   })
// MAGIC }
// MAGIC catch {  
// MAGIC   case ex : Throwable =>  {
// MAGIC     if (ex.getCause != None.orNull)
// MAGIC       errMEssage = ex.getCause.toString
// MAGIC     else
// MAGIC       errMEssage = ex.getMessage
// MAGIC     dbutils.notebook.exit(    
// MAGIC       """
// MAGIC         {
// MAGIC           "cmd": 5,
// MAGIC           "status":0,
// MAGIC           "error":""""  + errMEssage + """"
// MAGIC     }
// MAGIC     """
// MAGIC     )            
// MAGIC     }       
// MAGIC }     

// COMMAND ----------

// DBTITLE 1,Build temp F_TableTSQLHighToLow
// MAGIC %scala
// MAGIC import org.apache.spark.sql
// MAGIC 
// MAGIC try {
// MAGIC       F_TableTSQLHightToLow.foreach (tbl => {
// MAGIC         if (!tbl.isEmpty) {
// MAGIC           var sqlDrop = tbl.split(";")(0)
// MAGIC           val tableName = sqlDrop.substring(sqlDrop.toLowerCase().indexOf("exists") + 6)  
// MAGIC           println("creating=>" +tableName) 
// MAGIC           var sqlCreate= tbl.split(";")(1).toLowerCase()         
// MAGIC           
// MAGIC           /*
// MAGIC           sqlCreate = sqlCreate.replace("jedi_cubes.`g_", "`g_" )
// MAGIC           sqlCreate = sqlCreate.replace("jedi_cubes.g_", "g_" )          
// MAGIC           
// MAGIC           sqlCreate = sqlCreate.replace("jedi_cubes.`f_", "`f_" )
// MAGIC           sqlCreate = sqlCreate.replace("jedi_cubes.f_", "f_" )                              
// MAGIC           */
// MAGIC           spark.sql(sqlDrop) 
// MAGIC     
// MAGIC           val resGTableDF =  spark.sql(sqlCreate)//.repartition($"GridNum10km",$"GridNum")    
// MAGIC           resGTableDF.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tableName)       
// MAGIC         }
// MAGIC       })  
// MAGIC   
// MAGIC }
// MAGIC catch {  
// MAGIC   case ex : Throwable =>  {
// MAGIC     if (ex.getCause != None.orNull)
// MAGIC       errMEssage = ex.getCause.toString
// MAGIC     else
// MAGIC       errMEssage = ex.getMessage
// MAGIC     dbutils.notebook.exit(    
// MAGIC       """
// MAGIC         {
// MAGIC           "cmd": 6,
// MAGIC           "status":0,
// MAGIC           "error":""""  + errMEssage + """"
// MAGIC     }
// MAGIC     """
// MAGIC     )            
// MAGIC     }       
// MAGIC }     
// MAGIC 
// MAGIC /*
// MAGIC 
// MAGIC F_TableTSQLScale.foreach (tbl => {
// MAGIC   if (!tbl.isEmpty) {
// MAGIC     var sqlDrop = tbl.split(";")(0)
// MAGIC     var tableName =sqlDrop.substring(sqlDrop.toLowerCase().indexOf("exists") + 6).trim()
// MAGIC     spark.catalog.dropTempView(tableName.replace("jedi_cubes.","").replace("`",""))
// MAGIC     println("drop table if exists " + tableName)
// MAGIC   }
// MAGIC })
// MAGIC 
// MAGIC G_TableTSQL.foreach (tbl => {
// MAGIC   var sqlDrop = tbl.split(";")(0)
// MAGIC   var tableName =sqlDrop.substring(sqlDrop.toLowerCase().indexOf("exists") + 6).trim()
// MAGIC   spark.sql("drop table if exists " + tableName) 
// MAGIC   spark.catalog.dropTempView(tableName.replace("jedi_cubes.","").replace("`",""))
// MAGIC   println("drop table if exists " + tableName)
// MAGIC   println(tableName.replace("jedi_cubes.",""))
// MAGIC })
// MAGIC 
// MAGIC G_HighToLow_TSQL.foreach (tbl => {
// MAGIC   if (!tbl.isEmpty) {
// MAGIC     var sqlDrop = tbl.split(";")(0)
// MAGIC     var tableName =sqlDrop.substring(sqlDrop.toLowerCase().indexOf("exists") + 6).trim()    
// MAGIC     spark.sql("drop table if exists " + tableName)       
// MAGIC     println("drop table if exists " + tableName)
// MAGIC 
// MAGIC   }
// MAGIC })  
// MAGIC 
// MAGIC */

// COMMAND ----------

// DBTITLE 1,Build F_MODE tables
// MAGIC %scala
// MAGIC import org.apache.spark.sql
// MAGIC 
// MAGIC try {
// MAGIC   F_TableTSQL_A_MODEs.foreach (tbl => {    
// MAGIC     var sqlDrop = tbl.split(";")(0) 
// MAGIC     if (!sqlDrop.isEmpty) {
// MAGIC       sqlContext.clearCache()
// MAGIC       val tableName =sqlDrop.substring(sqlDrop.toLowerCase().indexOf("exists") + 6).trim()
// MAGIC       var sqlCreate= tbl.split(";")(1)            
// MAGIC       spark.sql(sqlDrop)   
// MAGIC       //try to create the dataframe/table parts for the next step's big query   )
// MAGIC       println("creating=>" +tableName)    
// MAGIC       val resFTableDF =  spark.sql(sqlCreate) //.repartition($"GridNum10km",$"GridNum")    
// MAGIC       resFTableDF.write.format("delta").mode("overwrite").saveAsTable(tableName) 
// MAGIC 
// MAGIC       }
// MAGIC     }
// MAGIC   )
// MAGIC }
// MAGIC catch {  
// MAGIC   case ex : Throwable =>  {
// MAGIC     if (ex.getCause != None.orNull)
// MAGIC       errMEssage = ex.getCause.toString
// MAGIC     else
// MAGIC       errMEssage = ex.getMessage
// MAGIC     dbutils.notebook.exit(    
// MAGIC       """
// MAGIC         {
// MAGIC           "cmd": 7,
// MAGIC           "status":0,
// MAGIC           "error":""""  + errMEssage + """"
// MAGIC     }
// MAGIC     """
// MAGIC     )            
// MAGIC     }       
// MAGIC }     

// COMMAND ----------

// DBTITLE 1,Build Table_A partial dataframes
// MAGIC %scala
// MAGIC import org.apache.spark.sql
// MAGIC 
// MAGIC try
// MAGIC {
// MAGIC   F_TableTSQL_A_Parts.foreach (tbl => {    
// MAGIC     var sqlDrop = tbl.split(";")(0) 
// MAGIC     if (!sqlDrop.isEmpty) {
// MAGIC       val tableName =sqlDrop.substring(sqlDrop.toLowerCase().indexOf("exists") + 6).trim()
// MAGIC       val sqlCreate= tbl.split(";")(1)            
// MAGIC       spark.sql(sqlDrop)   
// MAGIC       //try to create the dataframe/table parts for the next step's big query   )
// MAGIC       println("creating=>" +tableName)    
// MAGIC       val resFTableDF =  spark.sql(sqlCreate) //.repartition($"GridNum10km",$"GridNum")   
// MAGIC       resFTableDF.write.format("delta").mode("overwrite").saveAsTable(tableName) 
// MAGIC     }
// MAGIC   })
// MAGIC }
// MAGIC catch{
// MAGIC     case ex : Throwable =>  {
// MAGIC     if (ex.getCause != None.orNull)
// MAGIC       errMEssage = ex.getCause.toString
// MAGIC     else
// MAGIC       errMEssage = ex.getMessage
// MAGIC     dbutils.notebook.exit(    
// MAGIC       """
// MAGIC         {
// MAGIC           "cmd": 8,
// MAGIC           "status":0,
// MAGIC           "error":""""  + errMEssage + """"
// MAGIC     }
// MAGIC     """
// MAGIC     )            
// MAGIC     }
// MAGIC   
// MAGIC }
// MAGIC   

// COMMAND ----------

// DBTITLE 1,Build temp F_TableTSQL_A
// MAGIC %scala
// MAGIC var tableAQueries = F_TableTSQL_A.split(";") 
// MAGIC var sqlDrop = tableAQueries(0)
// MAGIC var tableName =sqlDrop.substring(sqlDrop.toLowerCase().indexOf("exists") + 6)
// MAGIC var sqlCreate= tableAQueries(1)
// MAGIC var TempTableAName = ""
// MAGIC try{
// MAGIC      if (!sqlDrop.isEmpty) {
// MAGIC        println("creating=>" +tableName)  
// MAGIC        spark.sql(sqlDrop) 
// MAGIC        var df = spark.emptyDataFrame;
// MAGIC        //If table_a is has a RENAME, it comes from hightolow table, and it is stored in the database. Otherwise, create a temp table. Temp table can't have jedi_cubes. prefix
// MAGIC        
// MAGIC        if (sqlCreate.indexOf("RENAME") == -1) {
// MAGIC          df =  spark.sql(sqlCreate)  
// MAGIC          TempTableAName = tableName.replace("jedi_cubes.","")
// MAGIC          df.createOrReplaceTempView(TempTableAName)
// MAGIC         //df.repartition($"GridNum10km",$"GridNum").write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tableName)     
// MAGIC        }
// MAGIC        else
// MAGIC        {
// MAGIC          df = spark.sql(sqlCreate)   
// MAGIC        }
// MAGIC      }
// MAGIC   
// MAGIC   }
// MAGIC catch {  
// MAGIC      case ex : Throwable =>  {
// MAGIC        if (ex.getCause != None.orNull)
// MAGIC          errMEssage = ex.getCause.toString
// MAGIC        else
// MAGIC          errMEssage = ex.getMessage
// MAGIC        dbutils.notebook.exit(    
// MAGIC          """
// MAGIC            {
// MAGIC              "cmd": 9,
// MAGIC              "status":0,
// MAGIC              "error":""""  + errMEssage + """"
// MAGIC        }
// MAGIC        """
// MAGIC        )            
// MAGIC      }
// MAGIC  }

// COMMAND ----------

// DBTITLE 1,Define auxiliar function to calculate Latitude from GridNum 
// MAGIC %scala
// MAGIC import org.apache.spark.sql.functions.{col, udf}
// MAGIC 
// MAGIC 
// MAGIC val GridNum2Lat = udf((GridNum:Long) => {
// MAGIC 	var x1:Double= 0
// MAGIC 	var y1:Double = 0
// MAGIC 	var i:Integer = 1
// MAGIC 	var FE:Double = 4321000
// MAGIC 	var FN:Double = 3210000
// MAGIC 	var Rq:Double = 6371007.1808834 // @semi_major_axis * sqrt(@qp / 2)
// MAGIC 	var D:Double = 1.00042539452802 //@semi_major_axis * cos(@lat0) / sqrt(1 - @esin0 * @esin0) / @Rq / cos(@beta0)
// MAGIC 	var qp:Double = 1.99553108748086 //1 - (1 - @e2) / 2 / @e * log((1 - @e) / (1 + @e))
// MAGIC 	var beta0:Double = 0.905397516815841 // asin(@q0 / @qp)
// MAGIC 	var e:Double = 0.0818191910435
// MAGIC 	var e2:Double = 0.00669438002301275 //power(@e,2)
// MAGIC 	var lon0:Double = 0.174532925199433 //radians(10.0)
// MAGIC 
// MAGIC 	//input
// MAGIC 	var x:Double = 4380218.5130047677
// MAGIC 	var y:Double = 3878010.3974277996
// MAGIC 	var rho:Double = 0
// MAGIC 	var C:Double= 0
// MAGIC 	var q:Double= 0
// MAGIC 	var phiOld:Double= 0
// MAGIC 	var sinPhiOld:Double= 0
// MAGIC 	var phi:Double= 0
// MAGIC 	var lat:Double= 0
// MAGIC 	var lon:Double= 0
// MAGIC 	var asinvalue:Double= 0
// MAGIC 
// MAGIC    
// MAGIC     //calculate laea_x coordinate from  gridnum
// MAGIC     var laea_x:Long =   ((GridNum / 0x10000000000000L) & 0x0f) * 1000000 +
// MAGIC                         ((GridNum / 0x00100000000000L) & 0x0f) *  100000 +       
// MAGIC                         ((GridNum / 0x00001000000000L) & 0x0f) *   10000 +
// MAGIC                         ((GridNum / 0x00000010000000L) & 0x0f) *    1000 +
// MAGIC                         ((GridNum / 0x00000000100000L) & 0x0f) *     100 +
// MAGIC                         ((GridNum / 0x00000000001000L) & 0x0f) *      10 +
// MAGIC                         ((GridNum / 0x00000000000010L) & 0x0f) *       1             
// MAGIC   
// MAGIC   
// MAGIC     //calculate laea_x coordinate from  gridnum
// MAGIC   var laea_y:Long =      ((GridNum / 0x1000000000000L) & 0x0f) * 1000000 +
// MAGIC                ((GridNum / 0x0010000000000L) & 0x0f) *  100000 +
// MAGIC                ((GridNum / 0x0000100000000L) & 0x0f) *   10000 +
// MAGIC                ((GridNum / 0x0000001000000L) & 0x0f) *    1000 +
// MAGIC                ((GridNum / 0x0000000010000L) & 0x0f) *     100 +
// MAGIC                ((GridNum / 0x0000000000100L) & 0x0f) *      10 +
// MAGIC                ((GridNum / 0x0000000000001L) & 0x0f) *       1
// MAGIC  
// MAGIC 	x1 = laea_x
// MAGIC 	y1 = laea_y
// MAGIC 	x = (x1 - FE) / D
// MAGIC 	y = (y1 - FN) * D
// MAGIC 	rho = Math.sqrt(x * x + y * y)
// MAGIC   
// MAGIC     
// MAGIC 	if (rho != 0) { 
// MAGIC 	
// MAGIC 		asinvalue = rho / 2 / Rq
// MAGIC 		C = 2 * Math.asin( if (Math.abs(asinvalue) > 1)  
// MAGIC                               if (asinvalue >0) {
// MAGIC                                 1
// MAGIC                               }
// MAGIC                               else if (asinvalue==0) {
// MAGIC                                 0
// MAGIC                               }
// MAGIC                               else {
// MAGIC                                 -1
// MAGIC                               }
// MAGIC                            else asinvalue )
// MAGIC 		q = qp * (Math.cos(C) * Math.sin(beta0) + y * Math.sin(C) * Math.cos(beta0) / rho)
// MAGIC 		phiOld = Math.asin(q/2)
// MAGIC 		sinPhiOld = Math.sin(phiOld)
// MAGIC 
// MAGIC 		phi = phiOld + Math.pow(1 - e2 * sinPhiOld * sinPhiOld, 2) / 2 / Math.cos(phiOld) * (q/(1 - e2) - sinPhiOld / (1 - e2 * sinPhiOld * sinPhiOld) + Math.log((1 - e * sinPhiOld)/(1 + e * sinPhiOld)) / 2 / e)
// MAGIC 
// MAGIC 		while (Math.abs(phi - phiOld) > 1e-14 && i  < 1000) {
// MAGIC 			i = i + 1
// MAGIC 			phiOld = phi
// MAGIC 			sinPhiOld = Math.sin(phiOld)
// MAGIC 			phi = phiOld + Math.pow(1 - e2 * sinPhiOld * sinPhiOld, 2) / 2 / Math.cos(phiOld) * (q/(1 - e2) - sinPhiOld / (1 - e2 * sinPhiOld * sinPhiOld) + Math.log((1 - e * sinPhiOld)/(1 + e * sinPhiOld)) / 2 / e)
// MAGIC         }
// MAGIC 
// MAGIC 		lat = Math.toDegrees(phi)
// MAGIC     }
// MAGIC 	else {
// MAGIC 		lat = 51.999999999479371	  
// MAGIC     }
// MAGIC   lat
// MAGIC })
// MAGIC spark.udf.register("GridNum2Lat", GridNum2Lat )

// COMMAND ----------

// DBTITLE 1,Define auxiliar function to calculate Longitude from GridNum 
// MAGIC %scala
// MAGIC import org.apache.spark.sql.functions.{col, udf}
// MAGIC 
// MAGIC 
// MAGIC val GridNum2Lon = udf((GridNum:Long) => {
// MAGIC 	var x1:Double = 0
// MAGIC 	var y1:Double = 0
// MAGIC 	var i:Integer = 1
// MAGIC 	var FE:Double = 4321000
// MAGIC 	var FN:Double = 3210000
// MAGIC 	var Rq:Double = 6371007.1808834 // @semi_major_axis * sqrt(@qp / 2)
// MAGIC 	var D:Double = 1.00042539452802 //@semi_major_axis * cos(@lat0) / sqrt(1 - @esin0 * @esin0) / @Rq / cos(@beta0)
// MAGIC 	var qp:Double = 1.99553108748086 //1 - (1 - @e2) / 2 / @e * log((1 - @e) / (1 + @e))
// MAGIC 	var beta0:Double = 0.905397516815841 // asin(@q0 / @qp)
// MAGIC 	var e:Double = 0.0818191910435
// MAGIC 	var e2:Double = 0.00669438002301275 //power(@e,2)
// MAGIC 	var lon0:Double = 0.174532925199433 //radians(10.0)
// MAGIC 
// MAGIC 	//input
// MAGIC 	var x:Double = 4380218.5130047677
// MAGIC 	var y:Double = 3878010.3974277996
// MAGIC 	var rho:Double = 0
// MAGIC 	var C:Double = 0
// MAGIC 	var q:Double = 0
// MAGIC 	var phiOld:Double = 0
// MAGIC 	var sinPhiOld:Double = 0
// MAGIC 	var phi:Double = 0
// MAGIC 	var lon:Double = 0
// MAGIC 	var asinvalue:Double = 0
// MAGIC 	
// MAGIC    
// MAGIC     //calculate laea_x coordinate from  gridnum
// MAGIC     var laea_x:Long =   ((GridNum / 0x10000000000000L) & 0x0f) * 1000000 +
// MAGIC                         ((GridNum / 0x00100000000000L) & 0x0f) *  100000 +       
// MAGIC                         ((GridNum / 0x00001000000000L) & 0x0f) *   10000 +
// MAGIC                         ((GridNum / 0x00000010000000L) & 0x0f) *    1000 +
// MAGIC                         ((GridNum / 0x00000000100000L) & 0x0f) *     100 +
// MAGIC                         ((GridNum / 0x00000000001000L) & 0x0f) *      10 +
// MAGIC                         ((GridNum / 0x00000000000010L) & 0x0f) *       1             
// MAGIC   
// MAGIC  
// MAGIC   
// MAGIC  
// MAGIC     //calculate laea_x coordinate from  gridnum
// MAGIC   var laea_y:Long =      ((GridNum / 0x1000000000000L) & 0x0f) * 1000000 +
// MAGIC                ((GridNum / 0x0010000000000L) & 0x0f) *  100000 +
// MAGIC                ((GridNum / 0x0000100000000L) & 0x0f) *   10000 +
// MAGIC                ((GridNum / 0x0000001000000L) & 0x0f) *    1000 +
// MAGIC                ((GridNum / 0x0000000010000L) & 0x0f) *     100 +
// MAGIC                ((GridNum / 0x0000000000100L) & 0x0f) *      10 +
// MAGIC                ((GridNum / 0x0000000000001L) & 0x0f) *       1
// MAGIC    
// MAGIC   
// MAGIC 	x1 = laea_x
// MAGIC 	y1 = laea_y
// MAGIC 	x = (x1 - FE) / D
// MAGIC 	y = (y1 - FN) * D
// MAGIC 	rho = Math.sqrt(x * x + y * y)
// MAGIC 	
// MAGIC 	if (rho != 0) {
// MAGIC         asinvalue = rho / 2 / Rq
// MAGIC 		C = 2 * Math.asin(  if (Math.abs(asinvalue) > 1)
// MAGIC                               if (asinvalue >0) {
// MAGIC                                 1
// MAGIC                               }
// MAGIC                               else if (asinvalue==0) {
// MAGIC                                 0
// MAGIC                               }
// MAGIC                               else {
// MAGIC                                 -1
// MAGIC                               }                          
// MAGIC 							else
// MAGIC                               asinvalue 
// MAGIC 						  )
// MAGIC 		lon = Math.toDegrees(lon0 + Math.atan(x * Math.sin(C) / (rho * Math.cos(beta0) * Math.cos(C) - y * Math.sin(beta0) * Math.sin(C)) ))
// MAGIC     }
// MAGIC 	else {
// MAGIC 		lon = 10	
// MAGIC     } 
// MAGIC 	lon  
// MAGIC   
// MAGIC })
// MAGIC spark.udf.register("GridNum2Lon", GridNum2Lon )

// COMMAND ----------

// DBTITLE 1,Method to delete _successXXX, _commitXX and other files generated when saving to DBFS
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

def removeLogFiles(path:String) = {
  /*
  val conf = sc.hadoopConfiguration
  val fs = FileSystem.get(conf)  
  var pathToFolder = new Path(path)
  val PathExists = new java.io.File(path).exists
  if(PathExists){
      val files = fs.listStatus(pathToFolder).map(_.getPath())   
      files.foreach(fp=> {
            if (fp.getName.toString.startsWith("_") ) {
              dbutils.fs.rm(fp.toString,true)
            }
        }            
    )  
  }
  */
  dbutils.fs.ls(path)
  .map(a => a.path.replace(path,"")).filter(b => b.startsWith("_"))
  .foreach(f=> {  
    dbutils.fs.rm( path + f ,true)
  })
   
}

// COMMAND ----------

// DBTITLE 1,Build F_TableTSQL
// MAGIC %scala
// MAGIC import org.apache.spark.sql
// MAGIC 
// MAGIC try{
// MAGIC    var sqlDrop = F_TableTSQL.split(";")(0)
// MAGIC   val tableName =sqlDrop.substring(sqlDrop.toLowerCase().indexOf("exists") + 6)
// MAGIC   val exportTableName = tableName.replace("jedi_cubes.","").replace("`","").trim()
// MAGIC   dbutils.fs.rm("dbfs:/mnt/trainingDatabricks/ExportTable/" + exportTableName ,true)
// MAGIC   var sqlCreate= F_TableTSQL.split(";")(1)
// MAGIC 
// MAGIC   //Remove jedi_cubes. from F_Table when F_Table_A is a Temp table (it does't have RENAME in its definition)
// MAGIC   val F_TableA_Create = F_TableTSQL_A.split(";")(1)
// MAGIC     
// MAGIC   if (F_TableA_Create.indexOf("RENAME") == -1) {
// MAGIC    
// MAGIC     sqlCreate = sqlCreate.replace("jedi_cubes.","")
// MAGIC     
// MAGIC   }
// MAGIC 
// MAGIC   //remove the associated files
// MAGIC   dbutils.fs.rm("dbfs:/mnt/trainingDatabricks/ExportTable/" + exportTableName, true)
// MAGIC   
// MAGIC   spark.sql(sqlDrop)  
// MAGIC    
// MAGIC   spark.sql(s"select GridNum10Km,GridNum,$Fields,longitude,latitude,AreaHa from ($sqlCreate)").repartition($"GridNum10km",$"GridNum")
// MAGIC     .write.format("com.databricks.spark.csv")
// MAGIC     .mode(SaveMode.Overwrite)
// MAGIC     .option("sep","|")
// MAGIC     .option("overwriteSchema", "true")
// MAGIC     .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
// MAGIC     .option("emptyValue", "")
// MAGIC     .option("treatEmptyValuesAsNulls", "true")
// MAGIC     .option("path","dbfs:/mnt/trainingDatabricks/ExportTable/" + exportTableName + "/")
// MAGIC     .saveAsTable(tableName)  
// MAGIC     
// MAGIC     removeLogFiles("dbfs:/mnt/trainingDatabricks/ExportTable/" + exportTableName + "/")
// MAGIC }
// MAGIC catch
// MAGIC {
// MAGIC   case ex : Throwable =>  {
// MAGIC     if (ex.getCause != None.orNull)
// MAGIC       errMEssage = ex.getCause.toString
// MAGIC     else
// MAGIC       errMEssage = ex.getMessage
// MAGIC     dbutils.notebook.exit(    
// MAGIC       """
// MAGIC         {
// MAGIC           "cmd": 13,
// MAGIC           "status":0,
// MAGIC           "error":""""  + errMEssage + """"
// MAGIC     }
// MAGIC     """
// MAGIC     )            
// MAGIC   }  
// MAGIC }

// COMMAND ----------

// DBTITLE 1,Build C MODEs tables
// MAGIC %scala
// MAGIC import org.apache.spark.sql
// MAGIC try 
// MAGIC {
// MAGIC   C_TableTSQL_MODEs.foreach (tbl => {    
// MAGIC     var sqlDrop = tbl.split(";")(0) 
// MAGIC     if (!sqlDrop.isEmpty) {
// MAGIC       sqlContext.clearCache()
// MAGIC       val tableName =sqlDrop.substring(sqlDrop.toLowerCase().indexOf("exists") + 6).trim()
// MAGIC       var sqlCreate= tbl.split(";")(1)      
// MAGIC       spark.sql(sqlDrop)   
// MAGIC 
// MAGIC         //try to create the dataframe/table parts for the next step's big query   )
// MAGIC         println("creating=>" +tableName)    
// MAGIC         val resCTableDF =  spark.sql(sqlCreate)
// MAGIC         resCTableDF.write.format("delta").mode("overwrite").saveAsTable(tableName) 
// MAGIC       }
// MAGIC   })
// MAGIC }
// MAGIC catch 
// MAGIC {  
// MAGIC   case ex : Throwable =>  {
// MAGIC       if (ex.getCause != None.orNull)
// MAGIC         errMEssage = ex.getCause.toString
// MAGIC       else
// MAGIC         errMEssage = ex.getMessage
// MAGIC       dbutils.notebook.exit(    
// MAGIC         """
// MAGIC           {
// MAGIC             "cmd": 14,
// MAGIC             "status":0,
// MAGIC             "error":""""  + errMEssage + """"
// MAGIC       }
// MAGIC       """
// MAGIC       )            
// MAGIC     }
// MAGIC }

// COMMAND ----------

// DBTITLE 1,Build C_TableTSQL
// MAGIC %scala
// MAGIC import org.apache.spark.sql
// MAGIC 
// MAGIC var numRecords:Long=0
// MAGIC var CTableName = ""
// MAGIC 
// MAGIC try {
// MAGIC   var sqlDrop = C_TableTSQL.split(";")(0)
// MAGIC   val tableName =sqlDrop.substring(sqlDrop.toLowerCase().indexOf("exists") + 6)
// MAGIC   val exportTableName = tableName.replace("jedi_cubes.","").replace("`","").trim()
// MAGIC   //remove the associated files
// MAGIC   dbutils.fs.rm("dbfs:/mnt/trainingDatabricks/ExportTable/" + exportTableName ,true)  
// MAGIC   var sqlCreate= C_TableTSQL.split(";")(1)
// MAGIC   spark.sql(sqlDrop) 
// MAGIC   CTableName = exportTableName
// MAGIC 
// MAGIC   var CTableDF = spark.sql(s"select $Fields,AreaHa FROM ($sqlCreate )")
// MAGIC   numRecords = CTableDF.count  
// MAGIC   CTableDF.write
// MAGIC     .format("com.databricks.spark.csv")
// MAGIC     .mode(SaveMode.Overwrite)
// MAGIC     .option("sep","|")
// MAGIC     .option("overwriteSchema", "true")
// MAGIC     .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
// MAGIC     .option("emptyValue", "")
// MAGIC     .option("treatEmptyValuesAsNulls", "true")  
// MAGIC     .option("path","dbfs:/mnt/trainingDatabricks/ExportTable/" + exportTableName + "/")
// MAGIC     .saveAsTable(tableName)  
// MAGIC     
// MAGIC     removeLogFiles("dbfs:/mnt/trainingDatabricks/ExportTable/" + exportTableName + "/")
// MAGIC }
// MAGIC catch {  
// MAGIC   case ex : Throwable =>  {
// MAGIC     if (ex.getCause != None.orNull)
// MAGIC       errMEssage = ex.getCause.toString
// MAGIC     else
// MAGIC       errMEssage = ex.getMessage
// MAGIC     dbutils.notebook.exit(    
// MAGIC       """
// MAGIC         {
// MAGIC           "cmd": 15,
// MAGIC           "status":0,
// MAGIC           "error":""""  + errMEssage + """"
// MAGIC     }
// MAGIC     """
// MAGIC     )            
// MAGIC     }       
// MAGIC }

// COMMAND ----------

// DBTITLE 1,Compute table stats for improving execution plans
// MAGIC %scala
// MAGIC //compute table stats
// MAGIC spark.sql("ANALYZE TABLE jedi_cubes."+ CTableName + " COMPUTE STATISTICS noscan") 
// MAGIC 
// MAGIC //compute column stats
// MAGIC val describeDF = spark.sql(s"DESCRIBE TABLE jedi_cubes.$CTableName").select("col_name")
// MAGIC 
// MAGIC val col_names = describeDF.filter("lower(col_name) not in ('x','y','gridnum','gridnum10km','areaha','','# partitioning','not partitioned')")
// MAGIC .map {
// MAGIC   case (Row(col_name:String)) => col_name
// MAGIC }.reduce( _ + "," + _ )
// MAGIC spark.sql(s"ANALYZE TABLE jedi_cubes.$CTableName  COMPUTE STATISTICS for columns $col_names")

// COMMAND ----------

// DBTITLE 1,Clean and refresh temp files, tables and dataframes
// MAGIC %scala
// MAGIC 
// MAGIC //try {
// MAGIC 
// MAGIC   //remove temp tables
// MAGIC   G_TableTSQL.foreach (tbl => {
// MAGIC     var sqlDrop = tbl.split(";")(0)
// MAGIC     tableName =sqlDrop.substring(sqlDrop.toLowerCase().indexOf("exists") + 6).trim()
// MAGIC     spark.sql("drop table if exists " + tableName) 
// MAGIC     spark.catalog.dropTempView(tableName.replace("jedi_cubes.","").replace("`",""))
// MAGIC     println("drop table if exists " + tableName)
// MAGIC     println(tableName.replace("jedi_cubes.",""))
// MAGIC 
// MAGIC   })
// MAGIC 
// MAGIC 
// MAGIC 
// MAGIC   F_TableTSQLScale_Part.foreach (tbl => {
// MAGIC     if (!tbl.isEmpty) {
// MAGIC       var sqlDrop = tbl.split(";")(0)
// MAGIC       var tableName =sqlDrop.substring(sqlDrop.toLowerCase().indexOf("exists") + 6).trim()
// MAGIC       spark.sql("drop table if exists " + tableName) 
// MAGIC       spark.catalog.dropTempView(tableName.replace("jedi_cubes.","").replace("`",""))
// MAGIC       println("drop table if exists " + tableName)
// MAGIC     }
// MAGIC   }) 
// MAGIC   
// MAGIC   F_TableTSQLScale.foreach (tbl => {
// MAGIC     if (!tbl.isEmpty) {
// MAGIC       var sqlDrop = tbl.split(";")(0)
// MAGIC       tableName =sqlDrop.substring(sqlDrop.toLowerCase().indexOf("exists") + 6).trim()
// MAGIC       spark.sql("drop table if exists " + tableName)     
// MAGIC       println("drop table if exists " + tableName)
// MAGIC 
// MAGIC     }
// MAGIC   })
// MAGIC 
// MAGIC   G_HighToLow_TSQL.foreach (tbl => {
// MAGIC     if (!tbl.isEmpty) {
// MAGIC       var sqlDrop = tbl.split(";")(0)
// MAGIC       tableName =sqlDrop.substring(sqlDrop.toLowerCase().indexOf("exists") + 6).trim()      
// MAGIC       spark.sql("drop table if exists " + tableName)       
// MAGIC       spark.catalog.dropTempView(tableName.replace("jedi_cubes.","").replace("`",""))
// MAGIC       println("drop table if exists " + tableName)
// MAGIC 
// MAGIC     }
// MAGIC   })  
// MAGIC   
// MAGIC   F_TableTSQLHightToLow.foreach (tbl => {
// MAGIC     if (!tbl.isEmpty) {
// MAGIC       var sqlDrop = tbl.split(";")(0)
// MAGIC       tableName =sqlDrop.substring(sqlDrop.toLowerCase().indexOf("exists") + 6).trim()
// MAGIC       spark.sql("drop table if exists " + tableName)      
// MAGIC           println("drop table if exists " + tableName)
// MAGIC 
// MAGIC     }
// MAGIC   })  
// MAGIC  //F_MODES
// MAGIC  F_TableTSQL_A_MODEs.foreach (tbl => {
// MAGIC     if (!tbl.isEmpty) {
// MAGIC       var sqlDrop = tbl.split(";")(0)
// MAGIC       tableName =sqlDrop.substring(sqlDrop.toLowerCase().indexOf("exists") + 6).trim()
// MAGIC       spark.sql("drop table if exists " + tableName) 
// MAGIC           println("drop table if exists " + tableName)
// MAGIC 
// MAGIC     }
// MAGIC   })
// MAGIC 
// MAGIC  F_TableTSQL_A_Parts.foreach (tbl => {
// MAGIC     if (!tbl.isEmpty) {
// MAGIC       var sqlDrop = tbl.split(";")(0)
// MAGIC       tableName =sqlDrop.substring(sqlDrop.toLowerCase().indexOf("exists") + 6).trim()
// MAGIC       spark.sql("drop table if exists " + tableName) 
// MAGIC           println("drop table if exists " + tableName)
// MAGIC 
// MAGIC     }
// MAGIC   })
// MAGIC 
// MAGIC   //Delete temp table A
// MAGIC   spark.catalog.dropTempView(TempTableAName)
// MAGIC 
// MAGIC   if (F_TableTSQL_A.length > 0  )  {
// MAGIC     var sqlTableA = F_TableTSQL_A.split(";")
// MAGIC     var sqlDrop = sqlTableA(sqlTableA.length-2)
// MAGIC     if (!sqlDrop.isEmpty) {
// MAGIC       tableName =sqlDrop.substring(sqlDrop.toLowerCase().indexOf("exists") + 6).trim()    
// MAGIC       spark.sql("drop table if exists " + tableName)    
// MAGIC           println("drop table if exists " + tableName)
// MAGIC 
// MAGIC     }
// MAGIC   }
// MAGIC   
// MAGIC 
// MAGIC   //C_MODES
// MAGIC    C_TableTSQL_MODEs.foreach (tbl => {
// MAGIC     if (!tbl.isEmpty) {
// MAGIC       var sqlDrop = tbl.split(";")(0)
// MAGIC       tableName =sqlDrop.substring(sqlDrop.toLowerCase().indexOf("exists") + 6).trim()
// MAGIC       spark.sql("drop table if exists " + tableName) 
// MAGIC       println(tableName)
// MAGIC 
// MAGIC     }
// MAGIC   })
// MAGIC 
// MAGIC   
// MAGIC   //clear cached elements
// MAGIC   for ( (id,rdd) <- sc.getPersistentRDDs ) {      
// MAGIC       rdd.unpersist()
// MAGIC   }
// MAGIC   
// MAGIC 
// MAGIC   //remove TSQL file
// MAGIC   val queryFile = dbutils.widgets.get("SQL_Queries_File")
// MAGIC   dbutils.fs.rm("dbfs:/mnt/trainingDatabricks/" + queryFile,true) 
// MAGIC   
// MAGIC 
// MAGIC   success =1
// MAGIC 
// MAGIC //}
// MAGIC //catch {
// MAGIC //  
// MAGIC //  case ex : Throwable =>  
// MAGIC //    {
// MAGIC //    if (ex.getCause != None.orNull)
// MAGIC //      errMEssage = ex.getCause.toString
// MAGIC //    else
// MAGIC //      errMEssage = ex.getMessage
// MAGIC //    }       
// MAGIC //           
// MAGIC //}
// MAGIC //
// MAGIC //finally {
// MAGIC 
// MAGIC   dbutils.notebook.exit("""{
// MAGIC         "cmd": 17,
// MAGIC         "status":""" +   success + """,
// MAGIC         "error":""""  + errMEssage + """",
// MAGIC         "num_records":"""  + numRecords + """
// MAGIC     }"""
// MAGIC   )
// MAGIC 
// MAGIC //}
// MAGIC //
// MAGIC //

// COMMAND ----------


