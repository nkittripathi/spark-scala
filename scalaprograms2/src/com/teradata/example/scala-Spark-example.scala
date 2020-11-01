package com.myorg.example
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import scala.Any

object playwithdataframes2 {
case class ZipTerr(zip_code: String, territory_code: String, 
    territory_name: String, state:String)

case class Key(zip_code: String, territory_code: String)

case class StudentRecord(Student: String, `Class`: String, Subject: String, Grade: String)

case class Person (ID:Int , name:String, age:Int, numFriends:Int)

def mapper (line :String): Person = {
		val fields = line.split(",")
				val person:Person = Person(fields(0).toInt,fields(1).toLowerCase(),fields(2).toInt,fields(3).toInt)
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

				val newData = spark.createDataFrame(List(
                                        ZipTerr("xxx1", "81A01", "TERR NAME 01", "NJ"),
                                        ZipTerr("xxx2", "81A01", "TERR NAME 01", "NJ"),
                                        ZipTerr("xxx3", "81A01", "TERR NAME 01", "NJ"),
                                        ZipTerr("xxx4", "81A01", "TERR NAME 01", "CA"),
                                        ZipTerr("xx5","81A01","TERR NAME 01","ME")
                                        ))

        val oldData = spark.createDataFrame(List(
                                        ZipTerr("xxx1","81A01","TERR NAME 55","NY"),
                                        ZipTerr("xxx2","81A01","TERR NAME 55","NY"),
                                        ))
        
        val testJson = spark.read.option("multiLine", true).option("mode", "PERMISSIVE").json("C:\\Users\\at186045\\Documents\\Project\\SparkStreaming\\test.json")
        newData.registerTempTable("tab1")
        oldData.registerTempTable("tab2")
        val result = newData.dropDuplicates("territory_code", "state")
        //val result = spark.sqlContext.sql("select distinct territory_code, state from tab1")
        val result1 = newData.select("territory_code", "state").distinct()
        
        val result2 = newData.groupBy("territory_code").count()
        val result3 = result.withColumn("NewCol", concat(lit("NewCol="), col("territory_code"))).select("NewCol")
        val testJson1 = testJson.select(explode(testJson("root.cust_info")).as("cust"))
                                .select("cust.Location", "cust.customer_id")
        
       // testJson.schema.foreach(println)
       
        import spark.implicits._
        val df = Seq( ("Sam", "6th Grade", "Maths", "A"),
                      ("Sam", "6th Grade", "Science", "A"),
                      ("Sam", "7th Grade", "Maths", "A-"),
                      ("Sam", "7th Grade", "Science", "A"),
                      ("Rob", "6th Grade", "Maths", "A"),
                      ("Rob", "6th Grade", "Science", "A-"),
                      ("Rob", "7th Grade", "Maths", "A-"),
                      ("Rob", "7th Grade", "Science", "B"),
                      ("Rob", "7th Grade", "AP", "A")).toDF("Student", "Class", "Subject", "Grade")
      
          

          val colList = df.columns
          //val df2 = df.map( x => { df.columns.indices.map(i=> lower(col(x.getAs(i))))})

           val df2 = df.columns.indices.map(i=> { col(df.columns(i))}) 
           val df4 = df.map(x => x.getValuesMap[String](colList))
           // Apply same transformation to all columns in a statement.
           val b = colList.foldLeft(df){(tempdf, colName) => tempdf.withColumn(colName, lower((col(colName))))}
		       b.show(false)
                     
                      
  }
}
