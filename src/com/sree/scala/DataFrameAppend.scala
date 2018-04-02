package com.sree.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import scala.reflect.ClassTag

case class Isbn(name: String, year: Int, isbn: String) {  override def toString: String = s"""($name, $year, $isbn)"""} 

object DataFrameAppend {
  
  def main(args: Array[String]): Unit = {
    
    implicit def kryoEncoder[Isbn](implicit ct: ClassTag[Isbn]) = org.apache.spark.sql.Encoders.kryo[Isbn](ct)
  
     val r1 = Isbn("Learning Spark: Lightning-Fast Big Data Analysis", 2015, "ISBN: 978-1449358624")

    val records = Seq(r1)
    
    //implicit val myObjEncoder = org.apache.spark.sql.Encoders.kryo[Isbn]
    
    val isbnEncoder = Encoders.product[Isbn]
     
    val sparksession = SparkSession.builder().appName("IsbnEncoderTest").master("local").getOrCreate();
    
    import sparksession.implicits._
    
    val sqlcontext = sparksession.sqlContext
    
    import sqlcontext.implicits._
    
    val df = sparksession.createDataset(records)
    
   // val rdd = df.javaRDD
    
   val df2 = df.flatMap(x => { List( (x.name, x.year, x.isbn), 
        (x.name, x.year, "ISBN-EAN:"+x.isbn.split(":")(1).split("-")(0)), 
        (x.name, x.year, "IISBN-GROUP:"+x.isbn.split(":")(1).split("-")(1).slice(0,2) ),
        (x.name, x.year, "ISBN-PUBLISHER:"+x.isbn.split(":")(1).split("-")(1).slice(2,6) ) ,
        (x.name, x.year, "ISBN-TITLE:"+x.isbn.split(":")(1).split("-")(1).slice(6,9)) )
        } )  
    
   val exploded_df = df2.map( x => Isbn.tupled(x).toString() )
   
   exploded_df.foreach(println(_))
    
  }
  
  
}