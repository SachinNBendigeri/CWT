package com.sachinbendigeri.spark.CWT.setup


import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j._
import com.sachinbendigeri.spark.CWT.utils.Utils
import com.sachinbendigeri.spark.CWT.constants.Constants
import scala.reflect.api.materializeTypeTag

object SparkSetup {
  /** ************************************************************
    * Set up Spark Context
    * *************************************************************/
  def GetSparkContext(AppName: String): SparkSession = {
    val sparkSession =  SparkSession
      .builder
      .appName(AppName)
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") 
      .enableHiveSupport()
      .getOrCreate()

    sparkSession.sql("set hive.exec.dynamic.partition=true")
    sparkSession.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sparkSession.sql("set hive.auto.convert.join=true")
    sparkSession.sql("set hive.enforce.bucketmap.join=true")
    sparkSession.sql("set hive.optimize.bucketmap.join=true")
    sparkSession.sql("set hive.exec.parallel=true")
    sparkSession.sql("set hive.execution.engine=tez")
    sparkSession.sql("set hive.vectorized.execution.enabled=true")
    sparkSession.sql("set hive.vectorized.execution.reduce.enabled=true")


    sparkSession.conf.set("hive.exec.dynamic.partition", "true")
    sparkSession.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    sparkSession.conf.set("hive.auto.convert.join", "true")
    sparkSession.conf.set("hive.enforce.bucketmap.join", "true")
    sparkSession.conf.set("hive.optimize.bucketmap.join", "true")
    sparkSession.conf.set("hive.exec.parallel", "true")
    sparkSession.conf.set("hive.execution.engine", "tez")
    sparkSession.conf.set("hive.vectorized.execution.enabled", "true")
    sparkSession.conf.set("hive.vectorized.execution.reduce.enabled", "true")

    sparkSession.conf.set("spark.broadcast.compress", "true")
    sparkSession.conf.set("spark.shuffle.compress", "true")
    sparkSession.conf.set("spark.shuffle.spill.compress", "true")
    sparkSession.conf.set("spark.hive.mapred.supports.subdirectories", "true")
    sparkSession.conf.set("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", "true")
    sparkSession.conf.set("spark.sql.broadcastTimeout", "3600")
    sparkSession.conf.set("spark.sql.orc.filterPushdown", "true")
    sparkSession.conf.set("spark.sql.orc.splits.include.file.footer", "true")
    sparkSession.conf.set("spark.sql.orc.cache.stripe.details.size", "10000")
    sparkSession.conf.set("spark.sql.hive.metastorePartitionPruning", "true")
    sparkSession.conf.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    sparkSession.conf.set("spark.hadoop.mapreduce.fileoutputcommitter.cleanup.skipped", "true")
    sparkSession
  }

    /**************************************************************
    * Reads the Parameter table into a Map for use by the rest of the
    * programs
    * *************************************************************/
  def ReadParametersTable(TableName: String, sparkSession: SparkSession): Map[String, String] = {
    ///Identify the CurrentDate
    var parameters = Map(Constants.PARAM_RUN_INSTANCE_TIME -> Utils.getCurrentTime)
    
    parameters  =parameters ++ Map(Constants.PARAM_VEHICLES_TABLE -> "CWT_VEHICLES") 
    parameters = parameters ++ Map(Constants.PARAM_ACCIDENTS_TABLE->"CWT_ACCIDENTS")
    parameters = parameters ++ Map(Constants.PARAM_CASUALTIES_TABLE->"CWT_CASUALTIES")
   
    parameters = parameters ++ Map(Constants.PARAM_REFERENCE_DATA_FOLDER -> "D:\\Contracting\\201909 - CWT Job Interview and Process\\ReferenceData")
    
    parameters = parameters ++ Map(Constants.PARAM_VEHICLES_DATA_FILE -> "D:\\Contracting\\201909 - CWT Job Interview and Process\\Vehicles0514.csv")
    parameters = parameters ++ Map(Constants.PARAM_ACCIDENTS_DATA_FILE -> "D:\\Contracting\\201909 - CWT Job Interview and Process\\Accidents0514.csv")
    parameters = parameters ++ Map(Constants.PARAM_CASUALTIES_DATA_FILE -> "D:\\Contracting\\201909 - CWT Job Interview and Process\\Casualties0514.csv")
    
    parameters = parameters ++ Map(Constants.PARAM_ACCIDENTSVEHICLESCASUALTIES_TABLE -> "CWT_ACCIDENTSVEHICLESCASULTIES_TABLE")

    
    /*val parameters = sparkSession.sql(
      "Select Field , " +
        " COALESCE(FieldValue,'') AS FieldValue  " +
        " From " + TableName +
        " where field is not null")
      .collect()
      .map(x => (x.get(0).toString.trim, x.get(1).toString.trim))
      .toMap
      * 
      */
    
    
    
    parameters
  }


   /** ************************************************************
    * Updates the Parameter table back into a HIVE table for other programs to use
    * programs
    * *************************************************************/
  def UpdateParametersTable(Parameters: Map[String, String], TableName: String, sparkSession: SparkSession): Unit = {
    sparkSession.createDataFrame(Parameters.toSeq)
      .toDF("Field", "FieldValue")
      .write.mode("Overwrite")
      .saveAsTable(TableName)
  }


}