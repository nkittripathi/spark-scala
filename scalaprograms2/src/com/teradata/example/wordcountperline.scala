package com.teradata.example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object wordcountperline {
  def main(args : Array[String])=
		{
        val sparkConf= new SparkConf()
				                    .setAppName("getHighestVal")
				                    .setMaster("local")
				                    .set("spark.testing.memory", "2147480000")
				val sparkContext = new SparkContext (sparkConf)
				val file = sparkContext.textFile("./input/wordcount.txt", 1)
        //val line = file.flatMap(line1 => line1.split("\n"))
        val line = file.map(line3 => (line3, line3.split(" ").length ))
        val filter = line.top(3)
        filter.foreach(println)
        println(filter)
        println(line)
        //val count = line.map(word => (word,1)).reduceByKey(_+_)
        /*val output = file.map(_.split(" ").map((_, 1)).groupBy(_._1)
                         .map { case (group: String, traversable) => traversable
                         .reduce{(a,b) => (a._1, a._2 + b._2)} }.toList)
                         .flatMap(tuple => tuple)
        * 
        */
        //filter.foreach(println)
        //line2.saveAsTextFile("./output/wordcount2")
				//val pairs = file.map(s => (s, 1))
        //val counts = pairs.reduceByKey((a, b) => a + b)
        //val count = file.map(word => (word,1))
        //counts.saveAsTextFile("./output/wordcount1")
		}
}