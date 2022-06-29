// Databricks notebook source
// DBTITLE 1,Register GridNumCalc(X,Y) function
// MAGIC %scala
// MAGIC import org.apache.spark.sql.functions.{col, udf}
// MAGIC 
// MAGIC val CalcGridNum = udf((x:Int,y:Int) => {
// MAGIC 
// MAGIC   (
// MAGIC      (x.toFloat / 1000000).floor.toLong  *  scala.math.pow(16,13).toLong +
// MAGIC      (y.toFloat / 1000000).floor.toLong     *  scala.math.pow(16,12).toLong +
// MAGIC      (x.toFloat / 100000 % 10).floor.toLong *  scala.math.pow(16,11).toLong +
// MAGIC      (y.toFloat / 100000 % 10).floor.toLong *  scala.math.pow(16,10).toLong +
// MAGIC      (x.toFloat /  10000 % 10).floor.toLong *  scala.math.pow(16,9).toLong  + 
// MAGIC      (y.toFloat /  10000 % 10).floor.toLong *  scala.math.pow(16,8).toLong  +
// MAGIC      (x.toFloat /   1000 % 10).floor.toLong *  scala.math.pow(16,7).toLong  + 
// MAGIC      (y.toFloat /   1000 % 10).floor.toLong *  scala.math.pow(16,6).toLong  +
// MAGIC      (x.toFloat /    100 % 10).floor.toLong *  scala.math.pow(16,5).toLong  +
// MAGIC      (y.toFloat /    100 % 10).floor.toLong *  scala.math.pow(16,4).toLong  +
// MAGIC      (x.toFloat /     10 % 10).floor.toLong *  scala.math.pow(16,3).toLong  +
// MAGIC      (y.toFloat /     10 % 10).floor.toLong *  scala.math.pow(16,2).toLong  +
// MAGIC      (x.toFloat          % 10).floor.toLong *  scala.math.pow(16,1).toLong  +
// MAGIC      (y.toFloat          % 10).floor.toLong
// MAGIC   ).toLong
// MAGIC   
// MAGIC } )
// MAGIC 
// MAGIC spark.udf.register("CalcGridNum", CalcGridNum)

// COMMAND ----------

// DBTITLE 1,Create custom exception class
// MAGIC %scala
// MAGIC final case class InvalidGeotiffException(private val message: String = "", 
// MAGIC                            private val cause: Throwable = None.orNull)
// MAGIC                       extends Exception(message, cause) 

// COMMAND ----------

// DBTITLE 1,Remove all temp DFs and files
// MAGIC %scala
// MAGIC import org.apache.hadoop.conf.Configuration
// MAGIC import org.apache.hadoop.fs.Path
// MAGIC import java.util.UUID.randomUUID
// MAGIC import org.apache.hadoop.fs.{FileSystem, Path }
// MAGIC 
// MAGIC 
// MAGIC def RemoveTempFilesAndDFs(files_metadata: scala.collection.immutable.Map[String,(List[(String,List[Double])], Int)],folderName:String, col_names: String) = {    
// MAGIC  implicit val sc: SparkContext = SparkContext.getOrCreate()
// MAGIC 
// MAGIC   //remove the files extracted from zip file. So as to leave the same files as before the execution of the notebook
// MAGIC   val conf = sc.hadoopConfiguration
// MAGIC   val fs = FileSystem.get(conf)
// MAGIC   val objpath =  new Path("dbfs:/mnt/trainingDatabricks/" + folderName +  "/")
// MAGIC 
// MAGIC   //get the files/directories present in the folder
// MAGIC   val files = fs.listStatus(objpath).map(_.getPath()) //.toString).filter (name => !name.contains(".zip"))  
// MAGIC   files.foreach ( fp=> 
// MAGIC             {
// MAGIC               if (fs.isDirectory(fp )) {
// MAGIC                 dbutils.fs.rm(fp.toString,true)
// MAGIC               }
// MAGIC               else {
// MAGIC                 if (!fp.getName.toString.contains(".zip") ) {
// MAGIC                   dbutils.fs.rm(fp.toString,true)
// MAGIC                 }
// MAGIC               }
// MAGIC             }
// MAGIC   ) 
// MAGIC   dbutils.fs.rm("dbfs:/mnt/trainingDatabricks/" + dbutils.widgets.get("FilesToCheck"),true)
// MAGIC      
// MAGIC   //remove the temp dataframes and temp views
// MAGIC   files_metadata.keys.map {f => {  
// MAGIC     spark.catalog.dropTempView( "`" + f +  "_temp`")  
// MAGIC     
// MAGIC   }}
// MAGIC       
// MAGIC   spark.catalog.dropTempView("gridNums")  
// MAGIC   sqlContext.clearCache()
// MAGIC   
// MAGIC }

// COMMAND ----------

// DBTITLE 1,Read input parameters and parse them
// MAGIC %scala
// MAGIC import org.json4s._
// MAGIC import org.json4s.jackson.JsonMethods._
// MAGIC import scala.io.Source 
// MAGIC 
// MAGIC implicit val formats = org.json4s.DefaultFormats
// MAGIC 
// MAGIC spark.conf.set("spark.sql.broadcastTimeout",  36000)
// MAGIC spark.conf.set("spark.sql.adaptive.enabled",  true)
// MAGIC spark.conf.set("spark.databricks.adaptive.autoOptimizeShuffle.enabled",true)
// MAGIC spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", true)
// MAGIC 
// MAGIC //spark.conf.set("spark.executor.memory","27g")
// MAGIC var errMEssage = "" 
// MAGIC 
// MAGIC //Location folder of the geotiffs in DBFS
// MAGIC val sourceFile =  dbutils.widgets.get("SourceFile")
// MAGIC val targetResolution= dbutils.widgets.get("Target_Resolution").toInt
// MAGIC val dimensionName= dbutils.widgets.get("Dimension_Name")
// MAGIC val folderName = sourceFile.split("/").dropRight(1).mkString("/")
// MAGIC var files_metadata:scala.collection.immutable.Map[String,(List[(String,List[Double])], Int)] = Map()
// MAGIC val broadCastTargetRes = sc.broadcast(targetResolution)
// MAGIC 
// MAGIC var files_metadata_str = dbutils.widgets.get("FilesToCheck")
// MAGIC if (files_metadata_str.contains(".json")) {
// MAGIC 
// MAGIC   val pathToFile =  "/dbfs/mnt/trainingDatabricks/" + files_metadata_str 
// MAGIC   val bufferedSource = Source.fromFile(pathToFile)
// MAGIC   files_metadata_str = bufferedSource.getLines.mkString
// MAGIC   bufferedSource.close
// MAGIC }
// MAGIC 
// MAGIC 
// MAGIC try {  
// MAGIC   files_metadata = parse(files_metadata_str).extract[List[JObject]].map  {
// MAGIC       jObj => {
// MAGIC          ((jObj \ "FieldName").extract[String],
// MAGIC           ((jObj \ "Files").extract[List[JObject]].map {
// MAGIC             jObj2 => {
// MAGIC             ((jObj2 \ "FileName").extract[String],(jObj2 \ "NoData").extract[List[Double]])    
// MAGIC             }      
// MAGIC           },(jObj \ "BitDepth").extract[Int])
// MAGIC          ) 
// MAGIC       }
// MAGIC   }.toMap
// MAGIC 
// MAGIC }
// MAGIC catch {  
// MAGIC   case ex : Throwable =>  {
// MAGIC     RemoveTempFilesAndDFs(files_metadata,folderName, "")
// MAGIC     if (ex.getCause != None.orNull)
// MAGIC       errMEssage = ex.getCause.toString
// MAGIC     else
// MAGIC       errMEssage = ex.getMessage
// MAGIC 
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

// DBTITLE 1,Drop multiraster dataframe to prevent from possible errors
// MAGIC %scala
// MAGIC spark.conf.set("spark.sql.broadcastTimeout",  36000)
// MAGIC spark.conf.set("spark.sql.adaptive.enabled",  true)
// MAGIC spark.conf.set("spark.databricks.adaptive.autoOptimizeShuffle.enabled",true)
// MAGIC 
// MAGIC //drop database table
// MAGIC spark.sql("DROP TABLE IF EXISTS jedi_dimensions." + dimensionName)
// MAGIC //remove the associated files
// MAGIC dbutils.fs.rm("dbfs:/mnt/trainingDatabricks/Dimensions/" + dimensionName, true)

// COMMAND ----------

// DBTITLE 1,Extract the files from uploaded zip file. Including subfolders and nested zip files
// MAGIC %python
// MAGIC from zipfile import ZipFile
// MAGIC import time
// MAGIC import json
// MAGIC import os
// MAGIC import uuid
// MAGIC 
// MAGIC def ExploreFolder(folder, targetPath):
// MAGIC   #navigate through the unzipped files and folders
// MAGIC   for root, dirs, files in os.walk(folder, topdown=False):
// MAGIC     for f in files:
// MAGIC       if (f.lower().endswith(".tif")):
// MAGIC         dbutils.fs.mv(root.replace("/dbfs","dbfs:") +  "/" + f, targetPath + "/" + f.replace(".TIF",".tif"))
// MAGIC 
// MAGIC       elif (f.endswith(".zip")):
// MAGIC         ExtractFiles(folder + "/" + f, targetPath) 
// MAGIC 
// MAGIC     #for name in dirs:
// MAGIC     #    ExploreFolder(folder + "/" +name , targetPath)
// MAGIC     #    dbutils.fs.rm(folder.replace("/dbfs","dbfs:") + "/" +name,True)
// MAGIC 
// MAGIC 
// MAGIC def ExtractFiles(zipPath, targetPath):  
// MAGIC   folderToUnzip = "/dbfs" +  targetPath +  "/" + str(uuid.uuid1())  
// MAGIC   print("Extracting " + zipPath)  
// MAGIC   with ZipFile(zipPath, 'r') as zipObj:
// MAGIC     # Extract all the contents of zip file in current directory
// MAGIC     zipObj.extractall(folderToUnzip)
// MAGIC 
// MAGIC   #wait a few seconds to make sure that the contents have been correctly unzipped
// MAGIC   time.sleep(30)  
// MAGIC   ExploreFolder(folderToUnzip, targetPath)
// MAGIC   dbutils.fs.rm(folderToUnzip.replace("/dbfs","dbfs:"),True )
// MAGIC 
// MAGIC try:
// MAGIC   sourceZip= dbutils.widgets.get("SourceFile")
// MAGIC 
// MAGIC   #extract the files in 
// MAGIC   zipPath = "/dbfs/mnt/trainingDatabricks/" +  sourceZip
// MAGIC   folderName = "/".join(sourceZip.split("/")[:-1])
// MAGIC 
// MAGIC   #navigate through the folder to extract all geotiffs from nested folders
// MAGIC   URI           = sc._gateway.jvm.java.net.URI
// MAGIC   Path          = sc._gateway.jvm.org.apache.hadoop.fs.Path
// MAGIC   FileSystem    = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
// MAGIC   Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration
// MAGIC 
// MAGIC   fs = FileSystem.get(Configuration())
// MAGIC   targetFolder = "/".join(sourceZip.split("/")[:-1])
// MAGIC   targetPath = "/mnt/trainingDatabricks/" + targetFolder
// MAGIC 
// MAGIC   ExtractFiles(zipPath, targetPath)   
// MAGIC 
// MAGIC 
// MAGIC except Exception as error:
// MAGIC     dbutils.notebook.exit(    
// MAGIC       json.dumps({"cmd":7, "status":0, "error": str(error) })
// MAGIC     )   

// COMMAND ----------

// DBTITLE 1,Method to delete _successXXX, _commitXX and other files generated when saving to DBFS
import org.apache.hadoop.fs.{FileSystem, Path}

def removeLogFiles(path:String) = {
  val conf = sc.hadoopConfiguration
  val fs = FileSystem.get(conf)  
  var pathToFolder = new Path(path)
  val files = fs.listStatus(pathToFolder).map(_.getPath())   
  files.foreach(fp=> {
                  if (fp.getName.toString.startsWith("_") ) {
                    dbutils.fs.rm(fp.toString,true)
                  }
              }            
    )   
}

// COMMAND ----------

// DBTITLE 1,Function to extract the pixels from RDD tiles. It creates an array of pixel values for each tile
// MAGIC %scala
// MAGIC /*
// MAGIC import io.circe.generic.auto._
// MAGIC import io.circe.syntax._
// MAGIC import io.circe.{Decoder, Encoder}
// MAGIC  */
// MAGIC import geotrellis.raster.Tile
// MAGIC import geotrellis.vector._
// MAGIC import geotrellis.vector.Extent
// MAGIC import org.apache.spark._
// MAGIC import org.apache.spark.rdd.RDD
// MAGIC import org.apache.spark.rdd._
// MAGIC 
// MAGIC 
// MAGIC def extractPixels(file:String,targetResolution: org.apache.spark.broadcast.Broadcast[Int], noData:List[Double], rdd: RDD[(ProjectedExtent, Tile)]): RDD[(String,Int,Int,Int,Int,List[Double], Array[Double])] = {
// MAGIC   
// MAGIC   val newRDD = rdd.collect { case (p, t) if !t.isNoDataTile =>     
// MAGIC    { 
// MAGIC     val minX:Int = (math.round(p.extent.xmin/targetResolution.value) * targetResolution.value).toInt  
// MAGIC     val maxY:Int = (math.round(p.extent.ymax/targetResolution.value) * targetResolution.value).toInt
// MAGIC     
// MAGIC     ( file, minX,maxY ,t.cols,t.rows,noData,  t.toArrayDouble)
// MAGIC     
// MAGIC    }
// MAGIC   }
// MAGIC   newRDD
// MAGIC }

// COMMAND ----------

// DBTITLE 1,Tile GeoTiffs with HadoopGeoTiffRDD.spatial. Create a Temp DF for each GeoTiff
// MAGIC %scala
// MAGIC import geotrellis.raster._
// MAGIC import geotrellis.raster.io.geotiff.reader.GeoTiffReader
// MAGIC import geotrellis.raster.io.geotiff._
// MAGIC import geotrellis.spark.store.hadoop.HadoopGeoTiffRDD
// MAGIC import geotrellis.vector._
// MAGIC import geotrellis.raster.reproject.Reproject
// MAGIC import org.apache.hadoop.fs.{FileSystem, Path}
// MAGIC import org.apache.log4j.{Logger, Level}
// MAGIC import geotrellis.proj4.{CRS, LatLng}
// MAGIC 
// MAGIC import org.apache.spark._
// MAGIC import spark.implicits._ 
// MAGIC import org.apache.spark.rdd.RDD
// MAGIC import org.apache.spark.sql.functions._
// MAGIC import org.apache.spark.HashPartitioner
// MAGIC import geotrellis.spark.tiling._
// MAGIC import geotrellis.raster.io.geotiff._
// MAGIC import geotrellis.raster.resample._
// MAGIC //import geotrellis.spark.store._
// MAGIC import scala.collection.mutable.ListBuffer
// MAGIC import org.apache.spark.sql._
// MAGIC import org.apache.spark.sql.types._
// MAGIC import scala.collection.mutable.WrappedArray
// MAGIC import org.apache.spark.storage.StorageLevel._
// MAGIC 
// MAGIC import org.apache.spark.sql.functions.{col, udf}
// MAGIC 
// MAGIC 
// MAGIC implicit val sc: SparkContext = SparkContext.getOrCreate( )
// MAGIC var success = 0
// MAGIC sc.setLogLevel("ERROR")
// MAGIC 
// MAGIC //spark.conf.set("spark.databricks.delta.optimize.zorder.checkStatsCollection.enabled",false)
// MAGIC 
// MAGIC try {
// MAGIC   
// MAGIC   val conf = sc.hadoopConfiguration
// MAGIC   val fs = FileSystem.get(conf)
// MAGIC   var pathToFolder = new Path("dbfs:/mnt/trainingDatabricks/" + folderName + "/" )
// MAGIC   
// MAGIC   //create a an array of files to be able to iterate them
// MAGIC   val files = fs.listStatus(pathToFolder).map(_.getPath().getName).filter(name=> name.toLowerCase.endsWith(".tif"))
// MAGIC   
// MAGIC   files_metadata.keys.par.foreach (
// MAGIC     f=> {
// MAGIC       
// MAGIC     //intialise the variables for the geotiff
// MAGIC     var (key:String, value :(List[(String,List[Double])], Int)) = files_metadata.head
// MAGIC     if  (files_metadata.size > 1) {
// MAGIC        key = f
// MAGIC     }
// MAGIC           
// MAGIC     val (_Files:List[(String,List[Double])] , _bitDepth:Int) = files_metadata(key)  
// MAGIC             
// MAGIC     //Create a list of RDD to make the union when iterated files  
// MAGIC     var FilesRDD = _Files.par.map(
// MAGIC       file=>{
// MAGIC         //get the path to open the geotiff file      
// MAGIC         var path = new Path("dbfs:/mnt/trainingDatabricks/" + folderName +  "/" + file._1.replace(".TIF",".tif"))
// MAGIC         if (!file._1.toLowerCase().endsWith(".tif")  ) {
// MAGIC           path = new Path("dbfs:/mnt/trainingDatabricks/" + folderName +  "/" + file._1 + ".tif")
// MAGIC         }          
// MAGIC         if (files.size == 1 ) {
// MAGIC           path = new Path("dbfs:/mnt/trainingDatabricks/" + folderName +  "/" + files.head.replace(".TIF",".tif"))
// MAGIC         }
// MAGIC         
// MAGIC         println("Path " + path)
// MAGIC         //open the geotiff
// MAGIC         var tileSize= 256      
// MAGIC         val numPartitions= 5000
// MAGIC         val opts = HadoopGeoTiffRDD.Options(maxTileSize = Some(tileSize), numPartitions = Some(numPartitions))
// MAGIC         
// MAGIC         var rdd: RDD[(ProjectedExtent, Tile)] = HadoopGeoTiffRDD.spatial(path, opts)  
// MAGIC                 
// MAGIC         //Create a RDD of type RDD[(XMin, YMax, Resx,ResY, NumCols, NumRows, [pixels])]      
// MAGIC         val dataTile:RDD[(String,Int,Int,Int,Int,List[Double], Array[Double])]= extractPixels(f, broadCastTargetRes,file._2, rdd )  
// MAGIC     
// MAGIC         //Return the RDD for each file        
// MAGIC         dataTile
// MAGIC          
// MAGIC       }
// MAGIC    )
// MAGIC       
// MAGIC     //Make the union to one single RDD so it can be aconverted to dataframe
// MAGIC     val FinalRDD = FilesRDD.reduce(_ union _)
// MAGIC     //Once we have a structured data structure we convert RDD to DataFrame to speed up the outsading conversions and to be able to save it
// MAGIC       //Afterwards:
// MAGIC       //1.explode pixel array to create a row per each pixel
// MAGIC       //2.remove NaN pixels  
// MAGIC       //3.generate gridnum
// MAGIC       //4.remove un-necessary columns since the gridnum and X,Y fields are created
// MAGIC     var extentDF = FinalRDD.toDF("file","Xmin","Ymax","cols","rows","noData","pixels")    
// MAGIC         .select($"file",$"Xmin",$"Ymax",$"Cols",$"Rows",$"noData", posexplode($"pixels"))
// MAGIC         .filter("!isnan(col)  and !array_contains(nodata, col)" )
// MAGIC         .withColumn("posX" , col("Xmin") + (col("pos")%col("cols")) * dbutils.widgets.get("Target_Resolution").toInt  )
// MAGIC         .withColumn("posY" , col("Ymax") - (floor(col("pos")/col("cols")) +1) * dbutils.widgets.get("Target_Resolution").toInt  )  
// MAGIC         .withColumn("gridnum", CalcGridNum(col("Xmin") + (col("pos")%col("cols")) * dbutils.widgets.get("Target_Resolution").toInt  ,  col("Ymax") - (floor(col("pos")/col("cols")) +1) * dbutils.widgets.get("Target_Resolution").toInt    ) )
// MAGIC     .drop("Xmin","ymax","cols","rows","pos","noData")
// MAGIC 
// MAGIC       //create a temp view to save the tiles per each geotiff (replace - character to _ to avoid SQL naming errors)
// MAGIC       extentDF.persist(MEMORY_AND_DISK)      
// MAGIC       println("`" + f +  "_temp` =>" + extentDF.count)  
// MAGIC 
// MAGIC       extentDF.createOrReplaceTempView( "`" + f +  "_temp`" )
// MAGIC   })
// MAGIC }
// MAGIC catch {
// MAGIC   
// MAGIC   case ex : Throwable =>  {
// MAGIC     RemoveTempFilesAndDFs(files_metadata,folderName,"")
// MAGIC     if (ex.getCause != None.orNull)
// MAGIC       errMEssage = ex.getCause.toString
// MAGIC     else
// MAGIC       errMEssage = ex.getMessage
// MAGIC     
// MAGIC     dbutils.notebook.exit(    
// MAGIC       """
// MAGIC         {
// MAGIC           "cmd": 9,
// MAGIC           "status":""" +   success + """,
// MAGIC           "error":""""  + errMEssage + """"
// MAGIC     }
// MAGIC     """
// MAGIC     )            
// MAGIC     }       
// MAGIC }

// COMMAND ----------

// DBTITLE 1,Combine temp DFs and save the results to Hive DataFrames (final dimension product)
// MAGIC %scala
// MAGIC var success = 0
// MAGIC var hasOverlaps:Long= 0
// MAGIC var numRecords:Long = 0
// MAGIC var resDF = spark.sql("select 1")
// MAGIC var numBuckets= 2000
// MAGIC 
// MAGIC try {
// MAGIC   if (files_metadata.size==1) {
// MAGIC     val f = files_metadata.keys.head 
// MAGIC     var sqlSelect = "select posX as x,posY as y, gridnum"
// MAGIC     var sqlFrom = " from `" + f + "_temp`"
// MAGIC 
// MAGIC     val (_Files:List[(String,List[Double])], _bitDepth:Int) = files_metadata(f)  
// MAGIC     if (_bitDepth == 1) {
// MAGIC       sqlSelect = sqlSelect + ",INT(`" + f + "_temp`.col) as `" + f+ "`, GridNum & cast(-4294967296 as bigint) as GridNum10km, pow( Int(" + targetResolution + ")/100,2) as AreaHa "
// MAGIC     }
// MAGIC     else {
// MAGIC       sqlSelect = sqlSelect + ",`" + f + "_temp`.col as `" + f + "`, GridNum & cast(-4294967296 as bigint) as GridNum10km, pow( Int(" + targetResolution + ")/100,2) as AreaHa"
// MAGIC     }        
// MAGIC     val sql = sqlSelect + " " + sqlFrom
// MAGIC     resDF=  spark.sql(sql)
// MAGIC   }
// MAGIC   else {
// MAGIC     var sql = files_metadata.keys.map {f => {  
// MAGIC       "select gridnum,posX,posY from `" + f +  "_temp`"  
// MAGIC     }}.reduce( _ + " UNION " + _ )
// MAGIC     val gridNums=  spark.sql(sql).cache()
// MAGIC     gridNums.createOrReplaceTempView( "gridNums" )
// MAGIC     
// MAGIC     var sqlSelect = "select gridnums.posX as x,gridnums.posY as y,gridnums.GridNum as GridNum"
// MAGIC     var sqlFrom = " from gridnums "
// MAGIC 
// MAGIC     files_metadata.keys.foreach {f => {
// MAGIC         val (_Files:List[(String,List[Double])], _bitDepth:Int) = files_metadata(f)  
// MAGIC 
// MAGIC         if (_bitDepth == 1) {         
// MAGIC           sqlSelect = sqlSelect + ",INT(`" + f + "_temp`.col) as `" + f  + "`"
// MAGIC         }
// MAGIC         else {
// MAGIC           sqlSelect = sqlSelect + ",`" + f + "_temp`.col as `" + f  + "`"
// MAGIC         }        
// MAGIC         sqlFrom = sqlFrom + " left join `" + f + "_temp` on gridnums.GridNum = `" + f + "_temp`.gridnum "  
// MAGIC       }
// MAGIC     }
// MAGIC     sqlSelect = sqlSelect + ",gridnums.gridnum & cast(-4294967296 as bigint) as GridNum10km,pow(Int(" + targetResolution + ")/100,2) as AreaHa "
// MAGIC     sql = sqlSelect + " " + sqlFrom
// MAGIC     resDF=  spark.sql(sql)  
// MAGIC   }
// MAGIC 
// MAGIC   resDF.repartition($"GridNum10km",$"GridNum")    
// MAGIC     .write
// MAGIC     //.format("parquet")
// MAGIC     .format("delta")
// MAGIC     //.bucketBy( numBuckets,"GridNum10km", "GridNum")
// MAGIC     //.sortBy( "GridNum10km", "GridNum")
// MAGIC     .mode(SaveMode.Overwrite)
// MAGIC     .option("path","dbfs:/mnt/trainingDatabricks/Dimensions/" + dimensionName + "/")
// MAGIC     .saveAsTable("jedi_dimensions." + dimensionName)  
// MAGIC 
// MAGIC   //get Dimension statistics
// MAGIC   //Num records
// MAGIC   val numRecordsDF=  spark.sql("select count(*) as Total from jedi_dimensions." + dimensionName)  
// MAGIC   numRecords= numRecordsDF.take(1)(0)(0).asInstanceOf[Number].longValue
// MAGIC 
// MAGIC   
// MAGIC   //has overlaps?
// MAGIC   val overlapsDF = spark.sql("SELECT SUM(TotalDups) as TotalDuplicates FROM (   SELECT count(*) as TotalDups   FROM jedi_dimensions." + dimensionName  + "  GROUP BY GridNum,GridNum10km  HAVING count(*) > 1)")
// MAGIC   hasOverlaps = if (overlapsDF.take(1)(0)(0)==null) 0 else overlapsDF.take(1)(0)(0).asInstanceOf[Number].longValue
// MAGIC   success= 1
// MAGIC 
// MAGIC }
// MAGIC catch {
// MAGIC   
// MAGIC   case ex : Throwable =>  {
// MAGIC     RemoveTempFilesAndDFs(files_metadata,folderName, "")
// MAGIC     if (ex.getCause != None.orNull)
// MAGIC       errMEssage = ex.getCause.toString
// MAGIC     else
// MAGIC       errMEssage = ex.getMessage
// MAGIC     dbutils.notebook.exit(    
// MAGIC       """
// MAGIC         {
// MAGIC           "cmd": 10,
// MAGIC           "status":""" +   success + """,
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
// MAGIC spark.sql("OPTIMIZE  jedi_dimensions."+ dimensionName + " ZORDER BY (GridNum10km)") 
// MAGIC spark.sql("ANALYZE TABLE jedi_dimensions."+ dimensionName + " COMPUTE STATISTICS noscan") 
// MAGIC  
// MAGIC //compute column stats
// MAGIC val describeDF = spark.sql(s"DESCRIBE TABLE jedi_dimensions.$dimensionName").select("col_name")
// MAGIC val col_names = describeDF.filter("lower(col_name) not in ('x','y','gridnum','gridnum10km','areaha','','# partitioning','not partitioned')")
// MAGIC .map {
// MAGIC   case (Row(col_name:String)) => col_name
// MAGIC }.reduce( _ + "," + _ )
// MAGIC spark.sql(s"ANALYZE TABLE jedi_dimensions.$dimensionName  COMPUTE STATISTICS for columns $col_names")
// MAGIC  

// COMMAND ----------

// DBTITLE 1,Remove temp files and exit notebook
// MAGIC %scala
// MAGIC //clear cached RDD for dimension DF
// MAGIC sc.getPersistentRDDs.foreach {
// MAGIC     (a) => {
// MAGIC       if (a._2.name.toUpperCase().contains(dimensionName.toUpperCase())) {
// MAGIC         println(a._2.name)
// MAGIC         a._2.unpersist()
// MAGIC       }
// MAGIC     }  
// MAGIC }  
// MAGIC 
// MAGIC RemoveTempFilesAndDFs(files_metadata,folderName,col_names)
// MAGIC 
// MAGIC dbutils.notebook.exit("""{
// MAGIC         "cmd": 12,
// MAGIC         "status":""" +   success + """,
// MAGIC         "error":""""  + errMEssage + """",
// MAGIC         "num_records":"""  + numRecords + """,
// MAGIC         "has_overlaps":"""  + hasOverlaps + """        
// MAGIC     }"""
// MAGIC )

// COMMAND ----------


