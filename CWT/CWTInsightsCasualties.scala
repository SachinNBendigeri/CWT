package com.sachinbendigeri.spark.CWT
import org.apache.spark.SparkContext._
import com.sachinbendigeri.spark.CWT.constants.Constants
import com.sachinbendigeri.spark.CWT.setup.SparkSetup
import com.sachinbendigeri.spark.CWT.ingestion.Vehicles
import com.sachinbendigeri.spark.CWT.utils.Utils
import org.apache.log4j._

object  CWTInsightsCasualties {
      def main(args: Array[String]) {
      
      // Set the Spark Context
      implicit val sparkSession = SparkSetup.GetSparkContext("CWTInsightsCasualties")
      import sparkSession.implicits._
      
      // Read Parameters from Hive table
      //var parameters = SparkSetup.ReadParametersTable(args(0), sparkSession)
      // Currently only read from hard coded values
      var parameters = SparkSetup.ReadParametersTable("Defalut", sparkSession)
      
      
      val CasualtyCategoryInsights = sparkSession.sql(""" 
        select Casualty_Severity_Type, count(distinct CasualtyID) as Casualty_Total,
         count(distinct case when Year = '2005' then CasualtyID end) as Casualty_2005,
         count(distinct case when Year = '2006' then CasualtyID end) as Casualty_2006,
         count(distinct case when Year = '2007' then CasualtyID end) as Casualty_2007,
         count(distinct case when Year = '2008' then CasualtyID end) as Casualty_2008,
         count(distinct case when Year = '2009' then CasualtyID end) as Casualty_2009,
         count(distinct case when Year = '2010' then CasualtyID end) as Casualty_2010,
         count(distinct case when Year = '2011' then CasualtyID end) as Casualty_2011,
         count(distinct case when Year = '2012' then CasualtyID end) as Casualty_2012,
         count(distinct case when Year = '2013' then CasualtyID end) as Casualty_2013,
         count(distinct case when Year = '2014' then CasualtyID end) as Casualty_2014         
        from """ + parameters(Constants.PARAM_ACCIDENTSVEHICLESCASUALTIES_TABLE) + """ a
        where AccidentPeakTimeFlag = 1
        group by Casualty_Severity_Type
        """).repartition(1)
        
        Utils.WriteToCSV(sparkSession,CasualtyCategoryInsights, "D:\\Contracting\\201909 - CWT Job Interview and Process\\CasualtyCategoryInsights.csv") 

      val CasualtyRoadTypeInsights = sparkSession.sql(""" 
        select First_Road_Class_Type, count(distinct CasualtyID) as Casualty_Total,
         count(distinct case when Year = '2005' then CasualtyID end) as Casualty_2005,
         count(distinct case when Year = '2006' then CasualtyID end) as Casualty_2006,
         count(distinct case when Year = '2007' then CasualtyID end) as Casualty_2007,
         count(distinct case when Year = '2008' then CasualtyID end) as Casualty_2008,
         count(distinct case when Year = '2009' then CasualtyID end) as Casualty_2009,
         count(distinct case when Year = '2010' then CasualtyID end) as Casualty_2010,
         count(distinct case when Year = '2011' then CasualtyID end) as Casualty_2011,
         count(distinct case when Year = '2012' then CasualtyID end) as Casualty_2012,
         count(distinct case when Year = '2013' then CasualtyID end) as Casualty_2013,
         count(distinct case when Year = '2014' then CasualtyID end) as Casualty_2014         
        from """ + parameters(Constants.PARAM_ACCIDENTSVEHICLESCASUALTIES_TABLE) + """ a
        where AccidentPeakTimeFlag = 1
        group by First_Road_Class_Type
        """).repartition(1)
        
        Utils.WriteToCSV(sparkSession,CasualtyRoadTypeInsights, "D:\\Contracting\\201909 - CWT Job Interview and Process\\CasualtyRoadTypeInsights.csv") 
        

      }
}