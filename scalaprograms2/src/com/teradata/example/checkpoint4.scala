package com.teradata.example

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructType, IntegerType, DateType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import java.util.Date; 
import java.text.SimpleDateFormat;  


object checkpoint4 {


val dtdiff =  (start_date: String, end_date: String)  => { 
        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");      
        val diff = format.parse(start_date).getTime() - format.parse(end_date).getTime()
        val diffInDays = diff / (1000 * 60 * 60 * 24);
        diffInDays.toInt
}

def main(args : Array[String])=
  {
		val spark = SparkSession
				.builder()
				.appName("Spark-Checkpoint4")
				.master("local")
				.config("spark.sql.warehouse.dir", "file:///C:/temp")
				.getOrCreate()					
		    
		    val dateDiffFunc = spark.udf.register("DateDifference", dtdiff)
		    
				val customerDF = spark.read.format("csv")
				                            .option("quote", "")
				                            .option("inferSchema", false)
				                            .load("./input/checkpointdataset4.txt")
				
				val custDF = customerDF.withColumn("splittedCustomer", split(col("_c0"), "\\|"))
                          .select(
                                  col("splittedCustomer").getItem(0).as("id"),
                                  col("splittedCustomer").getItem(1).as("end_date"),
                                  col("splittedCustomer").getItem(2).as("start_date"),
                                  col("splittedCustomer").getItem(3).as("location")
                                  )
                              
        val targetCustDF = custDF.withColumn("splittedLocation", split(col("location"), "\\-"))
                          .select(
                                  col("id"),
                                  col("end_date"),
                                  col("start_date"),
                                  col("splittedLocation").getItem(0).as("state"),
                                  col("splittedLocation").getItem(1).as("city")
                                  )
        
        // Applied udf in dataframe
        targetCustDF.select(col("id"),col("end_date"),col("start_date"),col("state"),col("city"),dateDiffFunc(col("end_date"),col("start_date")).as("number_of_days")).show()
        
        targetCustDF.createOrReplaceTempView("targetCust")
        //Applied udf in sql
        spark.sql("select id,end_date,start_date,state,city,DateDifference(end_date, start_date) AS number_of_days from targetCust").show()
						
  }
}
