package com.teradata.example

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}

object checkpoint1 {

val schema = new StructType().add("line", StringType)

def main(args : Array[String])=
  {
		val spark = SparkSession
				.builder()
				.appName("Spark-Checkpoint1")
				.master("local")
				.config("spark.sql.warehouse.dir", "file:///C:/temp")
				.getOrCreate()
				
				val webDf = spark.read.format("text").schema(schema).load("./input/checkpointdataset1.txt")
				
				//Get ip using regex
				val ipDF =webDf.withColumn("ip", regexp_extract(col("line"), "\\b\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\b", 0))			
				val resultDF =ipDF.groupBy(col("ip")).count().orderBy(col("count").desc).limit(5)
				resultDF.show()
			
  }
}
