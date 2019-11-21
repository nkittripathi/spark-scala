package com.teradata.example

import org.apache.spark._
import org.apache.spark.SparkConf

object test1 {
  
  case class Person (Name:String, Age:Int, City:String)    
  def mapper (line:String) : Person = {
    val fields = line.split(",")
    val person:Person = Person(fields(0).toString(),fields(1).toInt,fields(2).toString())
    return person
  }
    def main(args : Array[String])=
		{
        val sparkConf= new SparkConf()
				                    .setAppName("removeDuplicate")
				                    .setMaster("local")
				                    .set("spark.testing.memory", "2147480000")
			  
				val sparkContext = new SparkContext(sparkConf)
        val line = sparkContext.textFile("./input/test.txt")
        val header = line.first()
        val line1 = line.filter(x=> (x!=header))
        val fields = line1.map(x =>  (x.split(",").array(0) + "," + x.split(",").array(1), x.split(",").array(2))).reduceByKey(_+_)
        val dedup = fields
        //println(header)
       // dedup.foreach(println)
        //val header1 = sparkContext.parallelize(header)
       // header1.saveAsTextFile("./output/test1/")
        //header1.union(dedup).saveAsTextFile("./output/test1/")
      //  val fieldSplit = line.map(x=> (x.split(",").find
        

        
				                    
		}
}