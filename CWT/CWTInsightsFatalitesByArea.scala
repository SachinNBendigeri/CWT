package com.sachinbendigeri.spark.CWT
import org.apache.spark.SparkContext._
import com.sachinbendigeri.spark.CWT.constants.Constants
import com.sachinbendigeri.spark.CWT.setup.SparkSetup
import com.sachinbendigeri.spark.CWT.ingestion.Vehicles
import com.sachinbendigeri.spark.CWT.utils.Utils
import org.apache.log4j._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object  CWTInsightsFatalitiesByArea {
      def main(args: Array[String]) {
      
      // Set the Spark Context
      implicit val sparkSession = SparkSetup.GetSparkContext("CWTInsightsFatalitiesByArea")
      import sparkSession.implicits._
      
      // Read Parameters from Hive table
      //var parameters = SparkSetup.ReadParametersTable(args(0), sparkSession)
      // Currently only read from hard coded values
      var parameters = SparkSetup.ReadParametersTable("Defalut", sparkSession)
      
      val accidentsData = sparkSession.sql(""" 
        select Accident_Index, Local_Authority_District_Code, Date_Of_Accident from """ + parameters(Constants.PARAM_ACCIDENTS_TABLE) 
         ).repartition(25,$"Accident_Index")

      val casualtiesData = sparkSession.sql(""" 
        select Accident_Index, Vehicle_Reference,Casualty_Reference, Casualty_Severity_Code  from """ + parameters(Constants.PARAM_CASUALTIES_TABLE) 
         ).repartition(25,$"Accident_Index")         
      
      val Local_Authority_District= broadcast(sparkSession.sql(""" 
        select * from Local_Authority_District_Code"""))

      val Casualty_Severity= broadcast(sparkSession.sql(""" 
        select * from Casualty_Severity_Code"""))
        
      val exprs = Map("CasualtyID" -> "countDistinct", "CasualtyID2014" -> "countDistinct")  
      val FatalitiesByAreaInsights = accidentsData.join(casualtiesData,accidentsData.col("Accident_Index")===casualtiesData.col("Accident_Index"))
            .join(Local_Authority_District,accidentsData.col("Local_Authority_District_Code") ===Local_Authority_District.col("Type_Code"),"left")
            .join(Casualty_Severity,casualtiesData.col("Casualty_Severity_Code") ===Casualty_Severity.col("Type_Code"),"left")
            .withColumn("CasualtyID",when(Casualty_Severity.col("Type")=== "Fatal",
                                    concat(accidentsData.col("Accident_Index")
                                    ,casualtiesData.col("Vehicle_Reference")
                                    ,casualtiesData.col("Casualty_Reference"))
                                    ))
            .withColumn("Year",substring(accidentsData.col("Date_Of_Accident"),7,4))
            .withColumn("CasualtyID2014",when(Casualty_Severity.col("Type")=== "Fatal" && col("Year")==="2014", 
                                    concat(accidentsData.col("Accident_Index")
                                    ,casualtiesData.col("Vehicle_Reference")
                                    ,casualtiesData.col("Casualty_Reference"))
                                    ))
            .groupBy(Local_Authority_District.col("Type"))
            .agg(countDistinct("CasualtyID").as("TotalFatalities"),countDistinct("CasualtyID2014").as("Fatalities2014"))
           // .toDF("Local_Authority_District","CasualtyID","CasualtyID2014")
            .repartition(1)
        
        Utils.WriteToCSV(sparkSession,FatalitiesByAreaInsights, "D:\\Contracting\\201909 - CWT Job Interview and Process\\FatalitiesByAreaInsights.csv") 


      }
}