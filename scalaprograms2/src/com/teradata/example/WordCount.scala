package com.teradata.example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
object WordCount {
  def main(args : Array[String])=
		{
        val sparkConf= new SparkConf()
				                    .setAppName("getHighestVal")
				                    .setMaster("local")
				                    .set("spark.testing.memory", "2147480000")
				val sparkContext = new SparkContext (sparkConf)
				val file = sparkContext.textFile("./input/wordcount.txt", 1);
        val line = file.flatMap(line => line.split(" "))
        val count = line.map(word => (word,1)).reduceByKey(_+_)
        val count1 =count.map(x => (x._1, x._2))
        count1.sortBy(x => x._1).foreach(println)
        /* If we have one word per line
        val pairs = file.map(s => (s, 1))
        val counts = pairs.reduceByKey((a, b) => a + b)
        * 
        */
        //count.saveAsTextFile("./output/wordcount")
        println("------------------Count with Different Method-----------------")
        val line1 = file.flatMap(line => line.split("\\W+"))
        val countAgain = line1.countByValue()
        //countAgain.foreach(println)
		}
}