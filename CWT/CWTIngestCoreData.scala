package com.sachinbendigeri.spark.CWT
import org.apache.spark.SparkContext._
import com.sachinbendigeri.spark.CWT.constants.Constants
import com.sachinbendigeri.spark.CWT.setup.SparkSetup
import com.sachinbendigeri.spark.CWT.ingestion.Vehicles
import com.sachinbendigeri.spark.CWT.utils.Utils
import org.apache.log4j._

object  CWTIngestCoreData {
      def main(args: Array[String]) {
      
      // Set the Spark Context
      implicit val sparkSession = SparkSetup.GetSparkContext("CWTIngestCoreFactsData")
      import sparkSession.implicits._
      
      // Read Parameters from Hive table
      //var parameters = SparkSetup.ReadParametersTable(args(0), sparkSession)
      // Currently only read from hard coded values
      var parameters = SparkSetup.ReadParametersTable("Defalut", sparkSession)
      
      
      //Instantiate the Objects for Serialised processing
      val vehicles = new com.sachinbendigeri.spark.CWT.ingestion.Vehicles(sparkSession,parameters)
      val accidents = new com.sachinbendigeri.spark.CWT.ingestion.Accidents(sparkSession,parameters)
      val casualties = new com.sachinbendigeri.spark.CWT.ingestion.Casualties(sparkSession,parameters)
      
      
      //Full Refresh Vehicles Data
      val VehiclesData =vehicles.FullRefreshFromCSV(parameters(Constants.PARAM_VEHICLES_DATA_FILE))
      val VehiclesDataCount = VehiclesData.count()     
      println("--------------------------------------------------------------------------------------------------------------------------------------")
      println("The Vehicles Table :" + parameters(Constants.PARAM_VEHICLES_DATA_FILE) + " : Loaded into HIVE with counts of " + VehiclesDataCount )
      println("--------------------------------------------------------------------------------------------------------------------------------------")
      VehiclesData.show(20)
 
      //Full Refresh Accidents Data
      val AccidentsData =accidents.FullRefreshFromCSV(parameters(Constants.PARAM_ACCIDENTS_DATA_FILE))
      val AccidentsDataCount = AccidentsData.count()     
      println("--------------------------------------------------------------------------------------------------------------------------------------")
      println("The Accidents Table :" + parameters(Constants.PARAM_ACCIDENTS_TABLE) + " : Loaded into HIVE with counts of " + AccidentsDataCount )
      println("--------------------------------------------------------------------------------------------------------------------------------------")
      AccidentsData.show(20)      

      //Full Refresh Casualties Data
      val CasualtiesData =casualties.FullRefreshFromCSV(parameters(Constants.PARAM_CASUALTIES_DATA_FILE))
      val CasualtiesDataCount = CasualtiesData.count()     
      println("--------------------------------------------------------------------------------------------------------------------------------------")
      println("The Accidents Table :" + parameters(Constants.PARAM_CASUALTIES_TABLE) + " : Loaded into HIVE with counts of " + CasualtiesDataCount )
      println("--------------------------------------------------------------------------------------------------------------------------------------")
      CasualtiesData.show(20)            
      

      }
}