package com.teradata.example

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
//import org.apache.spark.SparkConf

object playwithdataframes {

case class Person (ID:Int , name:String, age:Int, numFriends:Int)

def mapper (line :String): Person = {
		val fields = line.split(",")
				val person:Person = Person(fields(0).toInt,fields(1),fields(2).toInt,fields(3).toInt)
				return person
}
def main(args : Array[String])=
  {
		val spark = SparkSession
				.builder()
				.appName("SparkDataFrameExample")
				.master("local")
				.config("spark.sql.warehouse.dir", "file:///C:/temp")
				.getOrCreate()

				//spark.conf.set("spark.testing.memory", "2147480000")
				//spark.conf.set("spark.driver.memory", "571859200")
				//spark.conf.set("spark.driver.host", "localhost")

				val line = spark.sparkContext.textFile("./input/fakefriends.csv")
				import spark.implicits._

				val people = line.map(mapper).toDF()
				people.printSchema()
				
				people.select("name").show()
				people.groupBy("age").count().sort("count").show()
  }
}