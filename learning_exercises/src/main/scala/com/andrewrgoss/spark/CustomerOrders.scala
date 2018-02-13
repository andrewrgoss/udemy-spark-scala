package com.andrewrgoss.spark

import org.apache.spark._
import org.apache.log4j._

/** Self-challenge: count up total amount spent (ordered) by each customer. */
// Split each comma-delimited line into fields
// Map each line to key/value pairs of customer ID (key) and dollar amount (value)
// Use reduceByKey to add up amount spent by customer ID
// Collect() the results and print them

object CustomerOrders {

  def parseLine(line:String)= {
    val fields = line.split(",")
    val customerID = fields(0).toInt
    val itemID = fields(1).toInt
    val spendAmt = fields(2).toFloat
    (customerID, itemID, spendAmt)
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "CustomerOrders")

    // Read each line of input data
    val lines = sc.textFile("../simple_examples/datasets/customer-orders.csv")

    // Convert to tuples
    val parsedLines = lines.map(parseLine)
    
  }

}