package com.teradata.example

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructType, IntegerType}
import org.apache.spark.sql.Row


object checkpoint2 {

val customerSchema = new StructType()
                      .add("customer_id", StringType)
                      .add("email_id", StringType)
                      .add("language", StringType)
                      .add("location", StringType)
                      
val purchaseSchema = new StructType()
                      .add("transaction_id", IntegerType)
                      .add("product_id", StringType)
                      .add("customer_id", StringType)
                      .add("sell_price", IntegerType)
                      .add("item_description", StringType)

def main(args : Array[String])=
  {
		val spark = SparkSession
				.builder()
				.appName("Spark-Checkpoint1")
				.master("local")
				.config("spark.sql.warehouse.dir", "file:///C:/temp")
				.getOrCreate()
							
	
				val customerDF = spark.read.format("csv").option("delimeted", ',').schema(customerSchema).load("./input/checkpointdataset21.txt")
				val purchaseDF = spark.read.format("csv").option("delimeted", ',').schema(purchaseSchema).load("./input/checkpointdataset22.txt")
			  val df = customerDF.join(purchaseDF, customerDF("customer_id") === purchaseDF("customer_id"),"inner")
			  val sumDF = df.groupBy(col("product_id"),col("location")).sum("sell_price").withColumnRenamed("sum(sell_price)", "total_sum")
			  val w = Window.partitionBy(col("product_id")).orderBy(col("total_sum").desc)
			  
			  // a) Find locations in which sale of each product is max
			  sumDF.withColumn("rnk", rank().over(w))
			      .filter(col("rnk")===lit(1))
			      .show()
			  
			  //b) Find customer who has purchased max number of items
			   val purDF = purchaseDF.groupBy(col("customer_id")).agg(count("product_id"), sum("sell_price"))
			            .withColumnRenamed("count(product_id)", "product_count")
			            .withColumnRenamed("sum(sell_price)", "sell_price_sum")
			   
			   purDF.orderBy(col("product_count").desc)
			        .limit(1)
			        .show()
			        
			  // c) Find customer who has spent max money
			   purDF.orderBy(col("sell_price_sum").desc)
			        .limit(1)
			        .show()
			        
			  // d) Find product which has min sale in terms of money
			   val minDF = purchaseDF.groupBy(col("product_id")).agg(sum("sell_price"), count("product_id"))
			                        .withColumnRenamed("count(product_id)", "product_count")
			                        .withColumnRenamed("sum(sell_price)", "sell_price_sum")
			   

			   minDF.orderBy(col("sell_price_sum").asc)
			        .limit(1)
			        .show()
			   
			  //e) Find product which has min sale in terms of number of unit sold
			   minDF.orderBy(col("product_count").asc)
			        .limit(1)
			        .show()	  
						
  }
}
