package com.teradata.example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object scalaconcepts {
  def main(args : Array[String])=
		{
       /*  val sparkConf= new SparkConf()
				                    .setAppName("getHighestVal")
				                    .setMaster("local")
				                    .set("spark.testing.memory", "2147480000")
				val sparkContext = new SparkContext (sparkConf)
				val file = sparkContext.textFile("./input/wordcount.txt", 1)
				* 
				*/
				val helo :String = "Hello"
				println(helo)
				val heloThr = helo + " World!!"
				println(heloThr)
				if (1>2)
				  println("True")
				else
				  println("False")
				  
       //----------------------Case Statement----------------------
				val number : Int =2
				number match {
				  case 1 => println("One")
				  case 2 => println("Two")
				  case _ => println("Default")
				}
			 //-------------------For Loop--------------------------------
				for (x <- 1 to 5) {
				  println("Square of " + x + " is " + x*x)
				}
				
			//---------Function Definition----
			 def squareIt(x: Int) : Int = {
			   x*x
			 }
			 println("Square of 5 is " + squareIt(5))
			 
		  //-----------Pass Function As Parameter-----------------------
			 
			 def funcAsParameter (x: Int, f:Int => Int) : Int = {
			   f(x)
			 }
			 println("In Function as Parameter, Square of 5 is " + funcAsParameter(5,squareIt))
			 println("In Function as Parameter, Cube of 5 is " + funcAsParameter(5,x => x*x*x))
			 
			 //--------Collections In Scala--------------------------------
			 //--------Tuple------------
			 val exampleList = ("item1","item2","item3")
			 println(exampleList._3)
			 
			 //--------Key/Value-------
			 val ship = "Key" -> "value"
			 println(ship._2)
			 
			 //--------List------------
			 val shipList = List("listItem1","listItem2","listItem3")
			 println(shipList(2))
			 println(shipList.head)
			 println(shipList.tail)
			 
			 //---------Iterating thru list------------
			 for (ship <- shipList) {
			   println("ShipList Item is " + ship)
			 }
			 
			 //------ reverse the string of item in list
			 val reverseList = shipList.map(ship => ship.reverse)
			 println("reversed string of item available in list " + reverseList)
			 
			 
			 //---------------------------
			 val intList = List(1,2,3,4,5,6)
			 val sum = intList.reduce(( x: Int, y: Int) => x+y)
			 println("Sum " + sum)
			 
			 //---------Map----------------
			 val myMap = Map("Key1" -> "value1", "Key2"-> "value2", "Key3" -> "Value3")
			 println("Get Value of Key Key1 " + myMap("Key2"))
			 println("Get Value of Key Key4 " + myMap.contains("Key4"))
			 
			 //---------Map with exception handling-------
			 val myval = util.Try(myMap("Key5")) getOrElse "Unknown" 
			 println(myval)
		}
  
}