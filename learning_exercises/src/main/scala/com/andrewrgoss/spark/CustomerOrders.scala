package com.andrewrgoss.spark

import org.apache.spark._
import org.apache.log4j._

/** Self-challenge: count up total amount spent (ordered) by each customer. */
// Split each comma-delimited line into fields
// Map each line to key/value pairs of customer ID (key) and dollar amount (value)
// Use reduceByKey to add up amount spent by customer ID
// Collect() the results and print them

object CustomerOrders {

  /** Convert input data to (customerID, amountSpent) tuples */
  def extractCustomerPricePairs(line: String) = {
    val fields = line.split(",")
    (fields(0).toInt, fields(2).toFloat)
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "CustomerOrders")

    // Read each line of input data
    val input = sc.textFile("../learning_exercises/datasets/customer-orders.csv")

    // Convert tuple to RDD key-value pair
    val mappedInput = input.map(extractCustomerPricePairs)

    // Reduce by customerID and sum the amount ordered by each customer
    val custOrders = mappedInput.reduceByKey((x, y) => x + y) // Combines values with the same key

    // Flip (customerID, amtSpent) tuples to (amtSpent, customerID) and then sort by key (the amount spent)
    val custAmtSpent = custOrders.map(x => (x._2, x._1)).sortByKey()

    // Print the results, flipping back (amtSpent, customerID) results to customerID: amtSpent
    val results = custAmtSpent.collect()

    for (result <- results.sorted) {
      val custAmtSpent = result._1
      val customerID = result._2
      println(s"customer ID: $customerID -- purchase total: $custAmtSpent")
    }

  }

}