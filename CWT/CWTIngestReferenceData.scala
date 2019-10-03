package com.sachinbendigeri.spark.CWT
import org.apache.spark.SparkContext._
import com.sachinbendigeri.spark.CWT.constants.Constants
import com.sachinbendigeri.spark.CWT.setup.SparkSetup
import com.sachinbendigeri.spark.CWT.ingestion.Vehicles
import com.sachinbendigeri.spark.CWT.utils.Utils
import org.apache.log4j._

object  CWTIngestReferenceData {
      def main(args: Array[String]) {
      
      // Set the Spark Context
      implicit val sparkSession = SparkSetup.GetSparkContext("VehicleProcessing")
      import sparkSession.implicits._
      // Read Parameters from Hive table
      //var parameters = SparkSetup.ReadParametersTable(args(0), sparkSession)
      // Currently only read from hard coded values
      var parameters = SparkSetup.ReadParametersTable("Defalut", sparkSession)
 
      //Instantiate the Objects for Serialised processing
      val References = new com.sachinbendigeri.spark.CWT.ingestion.ReferenceData(sparkSession, parameters)
      // Load all the files in the directory as reference data items
      val Ref = References.FullRefreshFromCSV(parameters(Constants.PARAM_REFERENCE_DATA_FOLDER))
      

      }
}