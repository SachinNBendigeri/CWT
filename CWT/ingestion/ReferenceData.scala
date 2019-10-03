package com.sachinbendigeri.spark.CWT.ingestion 

import com.sachinbendigeri.spark.CWT.constants.Constants
import com.sachinbendigeri.spark.CWT.utils.Utils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.Dataset
import org.apache.spark.SparkContext._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._
import org.slf4j.{Logger, LoggerFactory}



case class ReferenceDataStructure(
   Type_Code :Short,
   Type :String
)

class ReferenceData(sparkSession: SparkSession, parameters: Map[String, String]) extends Serializable {
  

   import sparkSession.implicits._

      //Logger to log messages
   val logger: Logger = LoggerFactory.getLogger(this.getClass.getSimpleName)
   
   def ReadFromCSV(FilePath: String): Dataset[ReferenceDataStructure] = {  
      val ReferenceDataSchema = ScalaReflection.schemaFor[ReferenceDataStructure].dataType.asInstanceOf[StructType]
      val df =  Utils.ReadFromCSV(sparkSession,FilePath,ReferenceDataSchema)                       
      return    df.as[ReferenceDataStructure]
   }
   
    def FullRefreshFromCSV(DirectoryPath :String) = {
      // Find all the files in the folder
      val ReferenceDataFiles = Utils.FilesInFolder(DirectoryPath)
      // for each file 
    for (i <- ReferenceDataFiles.indices) {
        // Read the data from the file
        var DS =  ReadFromCSV(ReferenceDataFiles(i)(0))
        // Load the Table reference table
        var DF =  Utils.OverwriteTable(sparkSession, DS.toDF(), ReferenceDataFiles(i)(1), parameters(Constants.PARAM_RUN_INSTANCE_TIME))
        var DFCounts = DF.count()
        println("---------------------------------------------------------------------------------------------------------")
        println("The Reference Table :" + ReferenceDataFiles(i)(1) + " : Loaded into HIVE with counts of " + DFCounts )
        println("---------------------------------------------------------------------------------------------------------")
      }
    }  
  
   
}   