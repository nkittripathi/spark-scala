package com.teradata.example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object sortSeq {
  
   def main(args : Array[String])=
		{
        val sparkConf= new SparkConf()
				                    .setAppName("getHighestVal")
				                    .setMaster("local")
				                    .set("spark.testing.memory", "2147480000")
				val sparkContext = new SparkContext (sparkConf)
				val file = sparkContext.textFile("./input/sortNumber.txt", 1)
        val result = file.countByValue();
        val sortedResult = result.toSeq.sortBy(_._1)
        sortedResult.foreach(println)
        val sortedResult1 = result.toSeq.sortBy(pair => pair._2)
        sortedResult1.foreach(println)
     
        val result1 = file.map(x => (1,x.toInt)).reduceByKey(_+_ )
        result1.foreach(println)
        
        def myFunc (x: String) = {
          
          val i = 1
          val str = "Hello"
          
          (i, str)
        }
		}
}