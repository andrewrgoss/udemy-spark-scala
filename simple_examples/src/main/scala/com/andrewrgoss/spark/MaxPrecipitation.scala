package com.andrewrgoss.spark

import org.apache.spark._
import org.apache.log4j._

/** Self-challenge: find the day that had the most precipitation from the 1800s weather data source  */
object MaxPrecipitation {
  
  def parseLine(line:String)= {
    val fields = line.split(",")
    val date = fields(1).toInt
    val entryType = fields(2)
    val precipitationAmt = fields(3).toInt
    (date, entryType, precipitationAmt)
  }
    /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MostPrecipitation")
    
    val lines = sc.textFile("../simple_examples/datasets/1800.csv")
    val parsedLines = lines.map(parseLine)
    val precipitationAmt = parsedLines.filter(x => x._2 == "PRCP")
    val datePrcp = precipitationAmt.map(x => (x._1, x._3.toInt))

    val maxPrcpAmt = datePrcp.reduce((x, y) => if(x._2 > y._2) x else y) // Max precipitation
    // returned as tuple with date key
    printf("The date with the most precipitation is: %s, with a precipitation amount of: %s",
      maxPrcpAmt._1, maxPrcpAmt._2)
  }
}