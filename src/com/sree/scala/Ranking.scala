package com.sree.scala

import org.apache.spark.sql.SparkSession

object Ranking {
  
  def main(args: Array[String]): Unit = {

    val sparksession = SparkSession.builder().appName("spark-rating").master("local[2]")getOrCreate()

    val dataFrame=sparksession.read.csv("/Users/sreekanthkarakula/Downloads/ml-latest-small/ratings.csv")

    //dataFrame.printSchema();
    
    
    println(dataFrame.count)
    
    import sparksession.implicits._
    
    val listofratings = dataFrame.map( x => x.toString().split(",")(2))
    
    val listoftuples = listofratings.toJavaRDD.countByValue();
    
    println(listoftuples)
    
   
    
  }
  
  
}