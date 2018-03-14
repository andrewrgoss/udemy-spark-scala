package com.andrewrgoss.spark

import org.apache.spark.sql._
import org.apache.log4j._

object SparkSQL {
  
  case class Person(ID:Int, name:String, age:Int, numFriends:Int)
  
  def mapper(line:String): Person = {
    val fields = line.split(',')  
    
    val person:Person = Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
    return person
  }
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      // .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug
      // in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
    
    val lines = spark.sparkContext.textFile("../advanced_examples/datasets/fakefriends.csv")
    val people = lines.map(mapper)
    
    // Infer the schema, and register the DataSet as a table.
    import spark.implicits._ // Needed for Spark to be able to convert a structured RDD into a dataset
    // (and avoid generating cryptic errors when using the .toDS function)
    val schemaPeople = people.toDS
    
    schemaPeople.printSchema()
    
    schemaPeople.createOrReplaceTempView("people")
    
    // SQL can be run over DataFrames that have been registered as a table
    val teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")
    
    val results = teenagers.collect()
    
    results.foreach(println)
    
    spark.stop() // Similar to opening and closing a database connection through a programming language.
    // Close the Spark session that was opened on ln24.
  }
}