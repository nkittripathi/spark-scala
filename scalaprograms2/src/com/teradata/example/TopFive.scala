package com.teradata.example

import org.apache.spark.SparkConf;
import java.io.PrintWriter

import java.io.File
import org.apache.spark.sql.SQLContext
import java.util.Arrays
import scala.collection.immutable.List

import org.apache.log4j;
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import scala.collection.mutable._


import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import java.util.Vector
import org.apache.spark.sql.Encoder
import java.util.stream.Collector

object TopFive {
  
  def main(args : Array[String])=
		{
				//System.setProperty("hadoop.home.dir", "C:\\temp\\winutils.exe");
				val sparkConf= new SparkConf()
				                    .setAppName("getHighestVal")
				                    .setMaster("local")
				                    .set("spark.testing.memory", "2147480000")
				val sparkContext = new SparkContext (sparkConf);
				val sqlContext= new SQLContext(sparkContext)
				val data=sqlContext.read.format("csv").option("delimeter", ",").option("header", true).load("./input/data.txt")
        data.show()
        val filtered_data=data.filter(x=> !x.toString().contains("null"))
        filtered_data.show();
				filtered_data.registerTempTable("city_details")
				val final_data= sqlContext.sql("select city_name,max(population)  from city_details group by city_name");
				final_data.show();
				final_data.rdd.coalesce(1).map(x=> x.mkString(",")).saveAsTextFile("hdfs://localhost:50071/tmp/test1");

		}

}