package com.andrewrgoss.spark

import org.apache.spark._
import org.apache.log4j._

// Challenge: find average number of friends by first name
/** Identify the average amount of social media friends for a given name. */
object FriendsByName {

  /** A function that splits a line of input into (name, numFriends) tuples. */
  def parseLine(line: String) = {
    // Split by commas
    val fields = line.split(",")
    // Extract the name and numFriends fields
    val name = fields(1)
    val numFriends = fields(3).toInt
    // Create a tuple that is our result.
    (name, numFriends) // This becomes our key-value pair for the key-value RDD that results from this operation.
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named FriendsByName
    val sc = new SparkContext("local[*]", "FriendsByName")

    // Load up each line of the source data into an RDD
    val lines = sc.textFile("../getting_started/datasets/fakefriends.csv")

    // Use our parseLines function to convert to (name, numFriends) tuples
    val rdd = lines.map(parseLine) // This is a key-value RDD

    // Lots going on here...
    // We are starting with an RDD of form (name, numFriends) where name is the KEY and numFriends is the VALUE
    // We use mapValues to convert each numFriends value to a tuple of (numFriends, 1)
    // Then we use reduceByKey to sum up the total numFriends and total instances for each name, by
    // adding together all the numFriends values and 1's respectively.
    val totalsByName = rdd.mapValues(x => (x, 1)).reduceByKey((x,y)=>(x._1 + y._1, x._2 + y._2)) // adds up all values for each unique key
    // ^ RDD[(String, (Int, Int))]
    //   RDD[(Name, (Total # friends, # instances))]

    // So now we have tuples of (name, (totalFriends, totalInstances))
    // To compute the average we divide totalFriends / totalInstances for each name.
    val averagesByName = totalsByName.mapValues(x => x._1 / x._2)

    // Collect the results from the RDD (This kicks off computing the DAG (Directed Acyclic Graph -- tasks execute transformations and actions on RDDs, equivalent of MapReduce) and actually executes the job)
    val results = averagesByName.collect()

    // Sort and print the final results.
    results.sorted.foreach(println)
  }
}

