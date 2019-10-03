package com.sachinbendigeri.spark.CWT.ingestion 

import com.sachinbendigeri.spark.CWT.constants.Constants
import com.sachinbendigeri.spark.CWT.utils.Utils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.Dataset
import org.apache.spark.SparkContext._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._
import org.slf4j.{Logger, LoggerFactory}

case class CasualtiesStructure(
        Accident_Index                        :String,
        Vehicle_Reference                     :String,
        Casualty_Reference                    :String,
        Casualty_Class_Code                   :Short,
        Sex_of_Casualty_Code                  :Short,
        Age_of_Casualty_Code                  :Short,
        Age_Band_of_Casualty                  :Short,
        Casualty_Severity_Code                :Short,
        Pedestrian_Location_Code              :Short,
        Pedestrian_Movement_Code              :Short,
        Car_Passenger_Code                    :Short,
        Bus_or_Coach_Passenger_Code           :Short,
        Pedestrian_Road_Maintenance_Worker_Code    :Short,
        Casualty_Type_Code                         :Short,
        Casualty_Home_Area_Type_Code               :Short
)


   
class Casualties(sparkSession: SparkSession, parameters: Map[String, String]) extends Serializable {
  

   import sparkSession.implicits._

   //Logger to log messages
   val logger: Logger = LoggerFactory.getLogger(this.getClass.getSimpleName)
  
        
   def ReadFromCSV(FilePath: String): Dataset[CasualtiesStructure] = {  
      val CasualtiesSchema = ScalaReflection.schemaFor[CasualtiesStructure].dataType.asInstanceOf[StructType]
      val df =  Utils.ReadFromCSV(sparkSession,FilePath,CasualtiesSchema)                       
      return    df.as[CasualtiesStructure]
   }
    
  def FullRefreshFromCSV(FilePath :String) = {
      val CasualtiesDS = ReadFromCSV(FilePath)
      Utils.OverwriteTable(sparkSession,CasualtiesDS.toDF(),parameters(Constants.PARAM_CASUALTIES_TABLE),parameters(Constants.PARAM_RUN_INSTANCE_TIME))  
   }  
    
    
   def DeltaRefreshFromCSV(FilePath :String) = {
      val CasualtiesDS = ReadFromCSV(FilePath)
      Utils.AppendTable(sparkSession,CasualtiesDS.toDF(),parameters(Constants.PARAM_CASUALTIES_TABLE),parameters(Constants.PARAM_RUN_INSTANCE_TIME))  
   }  
       
    


}