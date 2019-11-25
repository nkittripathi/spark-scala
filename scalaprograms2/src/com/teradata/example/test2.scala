package com.teradata.example

import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
//import org.apache.spark.sql.SparkSession.

object test2 {
  
  case class SessionInfo (sessiontime:String, username:String)    
  def mapper (line:String) : SessionInfo = {
    val fields = line.split(",")
    val sessionInfo:SessionInfo = SessionInfo(fields(0).toString(),fields(1).toString())
    return sessionInfo
  }
    def main(args : Array[String])=
		{

			  val sparkSession =  SparkSession
			                      .builder()
                            .appName("Active User Information")
                            .master("local[*]")
                            .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
                            .config("spark.testing.memory", "2147480000")
                            .getOrCreate()
                            
        val line = sparkSession.sparkContext.textFile("./input/test2.txt")
        val header = line.first()
        val noHeader = line.filter(x=> (x!=header))
        val sessionInfo = noHeader.map(mapper)
        sessionInfo.foreach(println)
        
        import sparkSession.implicits._
        
        val rows = sessionInfo.toDF()
        rows.printSchema()
        //val transformedRow = rows.withColumn("Sessiontime", (rows.col("sessiontime").cast("timestamp")))
        
        println("<--------------New Schema------------>")
        
        val x:Long = 1
        println("X is : - " + x/40)
       // transformedRow.printSchema()
        //transformedRow.show()
        val w = org.apache.spark.sql.expressions.Window.orderBy("sessiontime").partitionBy("username")
       // import org.apache.spark.sql.functions.lag
        //val transformedRows = transformedRow.groupBy("username")
        val leadDF = rows.withColumn("newSessiontime", lag("sessiontime",1,0).over(w))
        leadDF.printSchema()
        val transformedRow = leadDF.withColumn("sessiontime", (leadDF.col("sessiontime").cast("timestamp")))
                                    .withColumn("newSessiontime", (leadDF.col("newSessiontime").cast("timestamp")))
                                    .withColumn("SessionDiff", (col("sessiontime").cast("long") - col("newSessiontime").cast("long"))/60D)
                                    .withColumn("SessionId", when (col("SessionDiff").isNull,1).otherwise("2"))
        transformedRow.printSchema()
        transformedRow.show()
        

 
        

        
				                    
		}
}