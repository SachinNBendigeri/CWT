package com.sachinbendigeri.spark.CWT
import org.apache.spark.SparkContext._
import com.sachinbendigeri.spark.CWT.constants.Constants
import com.sachinbendigeri.spark.CWT.setup.SparkSetup
import com.sachinbendigeri.spark.CWT.ingestion.Vehicles
import com.sachinbendigeri.spark.CWT.utils.Utils
import org.apache.log4j._

object  CWTInsights {
      def main(args: Array[String]) {
      
      // Set the Spark Context
      implicit val sparkSession = SparkSetup.GetSparkContext("CWTInsights")
      import sparkSession.implicits._
      
      // Read Parameters from Hive table
      //var parameters = SparkSetup.ReadParametersTable(args(0), sparkSession)
      // Currently only read from hard coded values
      var parameters = SparkSetup.ReadParametersTable("Defalut", sparkSession)
      
      
      val AccidentAreasInsights = sparkSession.sql(""" 
        select /*+ MAPJOIN(LA,ASC)  */ 
         LA.Type as LocalAuthority,
         count(distinct Accident_Index) as Accidents,
         count(distinct case when trim(ASC.Type) = 'Fatal' then Accident_Index end) as Fatal_Accidents,
         count(distinct case when substr(Date_Of_Accident,7,4) = '2005' then Accident_Index end) as Accidents_2005,
         count(distinct case when substr(Date_Of_Accident,7,4) = '2006' then Accident_Index end) as Accidents_2006,
         count(distinct case when substr(Date_Of_Accident,7,4) = '2007' then Accident_Index end) as Accidents_2007,
         count(distinct case when substr(Date_Of_Accident,7,4) = '2008' then Accident_Index end) as Accidents_2008,
         count(distinct case when substr(Date_Of_Accident,7,4) = '2009' then Accident_Index end) as Accidents_2009,
         count(distinct case when substr(Date_Of_Accident,7,4) = '2010' then Accident_Index end) as Accidents_2010,
         count(distinct case when substr(Date_Of_Accident,7,4) = '2011' then Accident_Index end) as Accidents_2011,
         count(distinct case when substr(Date_Of_Accident,7,4) = '2012' then Accident_Index end) as Accidents_2012,
         count(distinct case when substr(Date_Of_Accident,7,4) = '2013' then Accident_Index end) as Accidents_2013,
         count(distinct case when substr(Date_Of_Accident,7,4) = '2014' then Accident_Index end) as Accidents_2014,
         count(distinct case when substr(Date_Of_Accident,7,4) = '2005' and trim(ASC.Type) = 'Fatal'  then Accident_Index end) as Fatal_Accidents_2005,
         count(distinct case when substr(Date_Of_Accident,7,4) = '2006' and trim(ASC.Type) = 'Fatal'  then Accident_Index end) as Fatal_Accidents_2006,
         count(distinct case when substr(Date_Of_Accident,7,4) = '2007' and trim(ASC.Type) = 'Fatal'  then Accident_Index end) as Fatal_Accidents_2007,
         count(distinct case when substr(Date_Of_Accident,7,4) = '2008' and trim(ASC.Type) = 'Fatal'  then Accident_Index end) as Fatal_Accidents_2008,
         count(distinct case when substr(Date_Of_Accident,7,4) = '2009' and trim(ASC.Type) = 'Fatal'  then Accident_Index end) as Fatal_Accidents_2009,
         count(distinct case when substr(Date_Of_Accident,7,4) = '2010' and trim(ASC.Type) = 'Fatal'  then Accident_Index end) as Fatal_Accidents_2010,
         count(distinct case when substr(Date_Of_Accident,7,4) = '2011' and trim(ASC.Type) = 'Fatal'  then Accident_Index end) as Fatal_Accidents_2011,
         count(distinct case when substr(Date_Of_Accident,7,4) = '2012' and trim(ASC.Type) = 'Fatal'  then Accident_Index end) as Fatal_Accidents_2012,
         count(distinct case when substr(Date_Of_Accident,7,4) = '2013' and trim(ASC.Type) = 'Fatal'  then Accident_Index end) as Fatal_Accidents_2013,
         count(distinct case when substr(Date_Of_Accident,7,4) = '2014' and trim(ASC.Type) = 'Fatal'  then Accident_Index end) as Fatal_Accidents_2014
         
        from """ + parameters(Constants.PARAM_ACCIDENTS_TABLE) + """ a
        left outer join Local_Authority_District_Code LA
        on A.Local_Authority_District_Code = LA.Type_Code
        left outer join Accident_Severity_Code ASC
        on A.Accident_Severity_Code = ASC.Type_Code
        group by LA.Type
        order by Fatal_Accidents desc
        """).repartition(1)
        
        Utils.WriteToCSV(sparkSession,AccidentAreasInsights, "D:\\Contracting\\201909 - CWT Job Interview and Process\\AccidentAreasInsights.csv") 


      }
}