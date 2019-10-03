package com.sachinbendigeri.spark.CWT.ingestion 

import com.sachinbendigeri.spark.CWT.constants.Constants
import com.sachinbendigeri.spark.CWT.utils.Utils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.Dataset
import org.apache.spark.SparkContext._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._
import org.slf4j.{Logger, LoggerFactory}

case class VehicleStructure(
        Accident_Index                       :String,
        Vehicle_Reference_Code               :Short,
        Vehicle_Type_Code                    :Short,
        Towing_and_Articulation_Code         :Short,
        Vehicle_Manoeuvre_Code               :Short,
        Vehicle_Location_Restricted_Lane_Code  :Short,
        Junction_Location_Code               :Short,
        Skidding_and_Overturning_Code        :Short,
        Hit_Object_in_Carriageway_Code       :Short,
        Vehicle_Leaving_Carriageway_Code     :Short,
        Hit_Object_off_Carriageway_Code      :Short,
        First_Point_of_Impact_Code             :Short,
        Was_Vehicle_Left_Hand_Drive_Code     :Short,
        Journey_Purpose_of_Driver_Code       :Short,
        Sex_of_Driver_Code                   :Short,
        Age_of_Driver                        :Short,
        Age_Band_of_Driver_Code              :Short,
        Engine_Capacity_CC                   :Int,
        Propulsion_Code                      :Short,
        Age_of_Vehicle                       :Short,
        Driver_IMD_Decile_Code               :Short,
        Driver_Home_Area_Type_Code           :Short
   )


   
class Vehicles(sparkSession: SparkSession, parameters: Map[String, String]) extends Serializable {
  

   import sparkSession.implicits._

   //Logger to log messages
   val logger: Logger = LoggerFactory.getLogger(this.getClass.getSimpleName)
  
        
   def ReadFromCSV(FilePath: String): Dataset[VehicleStructure] = {  
      val VehicleSchema = ScalaReflection.schemaFor[VehicleStructure].dataType.asInstanceOf[StructType]
      val df =  Utils.ReadFromCSV(sparkSession,FilePath,VehicleSchema)                       
      return    df.as[VehicleStructure]
   }
    
  def FullRefreshFromCSV(FilePath :String) = {
      val VehicleDS = ReadFromCSV(FilePath)
      Utils.OverwriteTable(sparkSession,VehicleDS.toDF(),parameters(Constants.PARAM_VEHICLES_TABLE),parameters(Constants.PARAM_RUN_INSTANCE_TIME))  
   }  
    
    
   def DeltaRefreshFromCSV(FilePath :String) = {
      val VehicleDS = ReadFromCSV(FilePath)
      Utils.AppendTable(sparkSession,VehicleDS.toDF(),parameters(Constants.PARAM_VEHICLES_TABLE),parameters(Constants.PARAM_RUN_INSTANCE_TIME))  
   }  
       
    


}