package com.sachinbendigeri.spark.CWT.ingestion 

import com.sachinbendigeri.spark.CWT.constants.Constants
import com.sachinbendigeri.spark.CWT.utils.Utils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.Dataset
import org.apache.spark.SparkContext._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._
import org.slf4j.{Logger, LoggerFactory}

case class AccidentStructure(
        Accident_Index        	     :String,
        Location_Easting_OSGR        :String,
        Location_Northing_OSGR       :String,
        Longitude                    :String,
        Latitude                     :String,
        Police_Force_Code            :Short,
        Accident_Severity_Code       :String,
        Number_of_Vehicles           :Short,
        Number_of_Casualties         :Short,
        Date_Of_Accident             :String ,
        Day_of_Week_Code             :Short,
        Time_Of_Accident              :String,
        Local_Authority_District_Code   :Short,
        Local_Authority_Highway_Code    :String,
        First_Road_Class_Code             :Short,
        First_Road_Number              :String,
        Road_Type_Code               :Short,
        Speed_limit                  :Int,
        Junction_Detail_Code         :Short,
        Junction_Control_Code        :Short,
        Second_Road_Class_Code          :Short,
        Second_Road_Number              :String,
        Pedestrian_Crossing_Human_Control_Code          :Short,
        Pedestrian_Crossing_Physical_Facilities_Code    :Short,
        Light_Conditions_Code        :Short,
        Weather_Conditions_Code      :Short,
        Road_Surface_Conditions_Code :Short,
        Special_Conditions_at_Site_Code   :Short,
        Carriageway_Hazards_Code          :Short,
        Urban_or_Rural_Area_Code          :Short,
        Did_Police_Officer_Attend_Scene_of_Accident_Code        :Short,
        LSOA_of_Accident_Location    :String
)



   
class Accidents(sparkSession: SparkSession, parameters: Map[String, String]) extends Serializable {
  

   import sparkSession.implicits._

   //Logger to log messages
   val logger: Logger = LoggerFactory.getLogger(this.getClass.getSimpleName)
  
        
   def ReadFromCSV(FilePath: String): Dataset[AccidentStructure] = {  
      val AccidentSchema = ScalaReflection.schemaFor[AccidentStructure].dataType.asInstanceOf[StructType]
      val df =  Utils.ReadFromCSV(sparkSession,FilePath,AccidentSchema)                       
      return    df.as[AccidentStructure]
   }
    
  def FullRefreshFromCSV(FilePath :String) = {
      val AccidentDS = ReadFromCSV(FilePath)
      Utils.OverwriteTable(sparkSession,AccidentDS.toDF(),parameters(Constants.PARAM_ACCIDENTS_TABLE),parameters(Constants.PARAM_RUN_INSTANCE_TIME))  
   }  
    
    
   def DeltaRefreshFromCSV(FilePath :String) = {
      val AccidentDS = ReadFromCSV(FilePath)
      Utils.AppendTable(sparkSession,AccidentDS.toDF(),parameters(Constants.PARAM_ACCIDENTS_TABLE),parameters(Constants.PARAM_RUN_INSTANCE_TIME))  
   }  
       
    


}