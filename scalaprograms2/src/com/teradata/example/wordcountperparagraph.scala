package com.teradata.example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object wordcountperparagraph {
  
   def main(args : Array[String])=
		{
        val sparkConf= new SparkConf()
				                    .setAppName("getHighestVal")
				                    .setMaster("local")
				                    .set("spark.testing.memory", "2147480000")
				val sparkContext = new SparkContext (sparkConf)
				val file = sparkContext.textFile("./input/wordcountperparagraph2.txt", 1)
        //val line = file.flatMap(line1 => line1.split("\n"))
				//val line2 = file.flatMap(line => line.splitAt(line.indexOf(</p>)))
        val line = file.map(line => line.split("</p>"))
        //val line1 = line.flatMap(line => line.)
        println(line)
        line.foreach(println)
        //val line1 = line.map(line1 => (line1, line1))
        //println(line)

		}
}