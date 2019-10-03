package com.sachinbendigeri.spark.CWT
import org.apache.spark.SparkContext._
import com.sachinbendigeri.spark.CWT.constants.Constants
import com.sachinbendigeri.spark.CWT.setup.SparkSetup
import com.sachinbendigeri.spark.CWT.ingestion.Vehicles
import com.sachinbendigeri.spark.CWT.utils.Utils
import org.apache.log4j._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object  CWTCurateCoreFactsData {
      def main(args: Array[String]) {
      
      // Set the Spark Context
      implicit val sparkSession = SparkSetup.GetSparkContext("CWTInsightsFatalitiesByArea")
      import sparkSession.implicits._
      
      // Read Parameters from Hive table
      //var parameters = SparkSetup.ReadParametersTable(args(0), sparkSession)
      // Currently only read from hard coded values
      var parameters = SparkSetup.ReadParametersTable("Defalut", sparkSession)
  
     //Read Reference data codes  
      val Local_Authority_District= broadcast(sparkSession.sql(""" select * from Local_Authority_District_Code"""))
      val First_Road_Class= broadcast(sparkSession.sql(""" select * from First_Road_Class_Code"""))
      val Vehicle_Manoeuvre= broadcast(sparkSession.sql(""" select * from Vehicle_Manoeuvre_Code"""))
      val Age_Band_of_Driver= broadcast(sparkSession.sql("""  select * from Age_Band_of_Driver_Code"""))       
      val Casualty_Severity= broadcast(sparkSession.sql("""  select * from Casualty_Severity_Code"""))
        
     //Read Core Facts Data     
      val accidentsData = sparkSession.sql(""" 
        select 
               Accident_Index, 
               Local_Authority_District_Code,
               Date_Of_Accident,
               Time_Of_Accident,
              from_unixtime(unix_timestamp(concat(Date_Of_Accident,' ',Time_Of_Accident),'dd/mm/yyyy HH:mm')) as Accident_Timestamp,
              Day_of_Week_Code, 
              First_Road_Class_Code 
        from """ + parameters(Constants.PARAM_ACCIDENTS_TABLE) 
         ).repartition(25,$"Accident_Index")

       val vehiclesData = sparkSession.sql(""" 
        select Accident_Index, Vehicle_Reference_Code as Vehicle_Reference, Vehicle_Manoeuvre_Code, Age_Band_of_Driver_Code  from """ + parameters(Constants.PARAM_VEHICLES_TABLE) 
         ).repartition(25,$"Accident_Index")

      val casualtiesData = sparkSession.sql(""" 
        select Accident_Index, Vehicle_Reference,Casualty_Reference, Casualty_Severity_Code  from """ + parameters(Constants.PARAM_CASUALTIES_TABLE) 
         ).repartition(25,$"Accident_Index")         
      

        
      //Join and curate the Core Facts Data    
      val AccidentVehicleCasualties = accidentsData
            .join(vehiclesData,accidentsData.col("Accident_Index")===vehiclesData.col("Accident_Index"))
            .join(casualtiesData,accidentsData.col("Accident_Index")===casualtiesData.col("Accident_Index") 
                              && vehiclesData.col("Vehicle_Reference") === casualtiesData.col("Vehicle_Reference") )
            .join(Local_Authority_District,accidentsData.col("Local_Authority_District_Code") ===Local_Authority_District.col("Type_Code"),"left")
            .join(First_Road_Class,accidentsData.col("First_Road_Class_Code") ===First_Road_Class.col("Type_Code"),"left")
            .join(Vehicle_Manoeuvre,vehiclesData.col("Vehicle_Manoeuvre_Code") ===Vehicle_Manoeuvre.col("Type_Code"),"left")
            .join(Age_Band_of_Driver,vehiclesData.col("Age_Band_of_Driver_Code") ===Age_Band_of_Driver.col("Type_Code"),"left")
            .join(Casualty_Severity,casualtiesData.col("Casualty_Severity_Code") ===Casualty_Severity.col("Type_Code"),"left")
            .withColumn("AccidentHour",substring(accidentsData.col("Time_Of_Accident"),1,2)) 
            .withColumn("WeekDay", when(col("Day_of_Week_Code") >= lit(2) && col("Day_of_Week_Code") <= lit(6),lit(1))
                                   )
            .withColumn("AccidentPeakTimeFlag",
                                   when(col("WeekDay") === lit(1) && col("AccidentHour") >= lit(7) && col("AccidentHour") < lit(10),lit(1))
                                   when(col("WeekDay") === lit(1) && col("AccidentHour") >= lit(16) && col("AccidentHour") < lit(19),lit(1))
                                   )
            .withColumn("CasualtyID",
                                    concat(accidentsData.col("Accident_Index")
                                    ,casualtiesData.col("Vehicle_Reference")
                                    ,casualtiesData.col("Casualty_Reference"))
                                    )
            .withColumn("Year",substring(accidentsData.col("Date_Of_Accident"),7,4))
            .withColumn("CasualtyID2014",when(col("Year")==="2014", 
                                    concat(accidentsData.col("Accident_Index")
                                    ,casualtiesData.col("Vehicle_Reference")
                                    ,casualtiesData.col("Casualty_Reference"))
                                    ))
            .select (
              accidentsData.col("Accident_Index").as("Accident_Index"),
              vehiclesData.col("Vehicle_Reference").as("Vehicle_Reference"),
              casualtiesData.col("Casualty_Reference").as("Casualty_Reference"),        
              Local_Authority_District.col("Type").as("Local_Authority_District_Type"),
              accidentsData.col("Date_Of_Accident").as("Date_Of_Accident"),
              accidentsData.col("Time_Of_Accident").as("Time_Of_Accident"),
              accidentsData.col("Accident_Timestamp").as("Accident_Timestamp"),
              accidentsData.col("Day_of_Week_Code").as("Day_of_Week"),        
              First_Road_Class.col("Type").as("First_Road_Class_Type"),
              Vehicle_Manoeuvre.col("Type").as("Vehicle_Manoeuvre_Type"),
              Age_Band_of_Driver.col("Type").as("Age_Band_of_Driver_Type"),        
              Casualty_Severity.col("Type").as("Casualty_Severity_Type"),
              col("CasualtyID"),
              col("Year"),
              col("CasualtyID2014"),
              col("AccidentHour"),
              col("AccidentPeakTimeFlag")   
            )
                  
          val DF = Utils.OverwriteTable(sparkSession, 
                              AccidentVehicleCasualties,
                              parameters(Constants.PARAM_ACCIDENTSVEHICLESCASUALTIES_TABLE), 
                              parameters(Constants.PARAM_RUN_INSTANCE_TIME))  
          DF.show(20)                    

      }
}