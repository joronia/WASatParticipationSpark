package com.jose.school.data

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.Column

object Main {
  def main(args : Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("SchoolDistrictData").getOrCreate();
    
    val satParticipation2018DF = spark.read.format("csv").option("header", "true").load("C:\\datasets\\2017\\wa-districtsummary2018 - WADistrictSummaryHighSchool_cro.csv")
    val satParticipation2017DF = spark.read.format("csv").option("header","true").load("C:\\datasets\\2017\\sat.csv")
    
    
    var df = satParticipation2017DF.select("WA  Public School Districts-2017","SAT Test Takers","Total Score Mean","ERW Mean","Math Mean").na.fill("0", Seq("Total Score Mean","ERW Mean","Math Mean"))
    df = df.withColumnRenamed("SAT Test Takers","SAT Test Takers 2017")
         .withColumnRenamed("Total Score Mean", "Total Score Mean 2017")
         .withColumnRenamed("ERW Mean", "ERW Mean 2017")
         .withColumnRenamed("Math Mean", "Math Mean 2017")
         
    var satScores2018DF = satParticipation2018DF.select("WA  Public School Districts-2018","SAT Test Takers","Total Score Mean","ERW Mean","Math Mean").na.fill("0", Seq("Total Score Mean","ERW Mean","Math Mean"))
    satScores2018DF = satScores2018DF.withColumnRenamed("SAT Test Takers","SAT Test Takers 2018")
         .withColumnRenamed("Total Score Mean", "Total Score Mean 2018")
         .withColumnRenamed("ERW Mean", "ERW Mean 2018")
         .withColumnRenamed("Math Mean", "Math Mean 2018")
    
    val joinExpression = df.col("WA  Public School Districts-2017") === satScores2018DF.col("WA  Public School Districts-2018")
    
    val df1 = df.join(satScores2018DF, joinExpression).drop("WA  Public School Districts-2017").withColumnRenamed("WA  Public School Districts-2018", "WA  Public School Districts")
    
    df1.write.format("csv").option("header", "true").save("C:\\datasets\\transformed\\wa_school_district_sat-participation")
  }
}