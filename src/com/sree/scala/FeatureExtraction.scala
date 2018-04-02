package com.sree.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.col

object FeatureExtraction {
  
    def main(args:Array[String]):Unit = {
    
    val spark = SparkSession.builder().master("local[*]").appName("feature extraction").getOrCreate();
    
    val sqlcontext = new org.apache.spark.sql.SQLContext(spark.sparkContext)
    
    val df = spark.read.format("csv").option("header" , "true").option("inferschema", "true").load("/Users/sreekanthKarakula/Desktop/test.csv")

    import spark.implicits._
    
    df.cache()
    
    val measurementdf = df.filter($"Type" === "Measurement")
    
    measurementdf.show()
    
    val testdf = df.filter(col("Type") === "Test")
    
    testdf.show()
    
    //testdf.groupBy(col("Name"), "","")
   
    val joineddf = measurementdf.join(testdf, measurementdf("Part") === testdf("Part"))
    
   // joineddf.groupBy(("Part","Location","Name"))
    
    joineddf.show()
   
  }
  
}