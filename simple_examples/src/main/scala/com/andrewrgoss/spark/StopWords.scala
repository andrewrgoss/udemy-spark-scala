package com.andrewrgoss.spark

import org.apache.spark._
import org.apache.log4j._

/** Count up how many of each word occurs in a book, using regular expressions and sorting the final results.
  * Self-challenge: introduce stop list of words, filter out common words of the English language.
  * Filter function that removes these from processing early on.
  * */
object StopWords {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using the local machine
    val sc = new SparkContext("local", "WordCountBetterSorted")

    // Create the RDDs
    val input = sc.textFile("../simple_examples/datasets/book.txt") // Book words
    val stopWordsInput = sc.textFile("../simple_examples/datasets/stop-word-list.csv") // Stop words

    // Flatten, collect, and broadcast
    val stopWords = stopWordsInput.flatMap(x => x.split(",")).map(_.trim) // Flatmap transforms an RDD of length N
    // into a collection of N collections, then flattens these into a single RDD of results
    val broadcastStopWords = sc.broadcast(stopWords.collect.toSet) // Broadcast variables allow you to keep a read-only variable cached on each machine
    // rather than shipping a copy with tasks

    // Split book words using a regular expression that extracts words
    val wordsWithStopWords = input.flatMap(x => x.split("\\W+"))

    // Filter out stop words, single characters, and numbers from the book words then normalize everything to lowercase
    val lowercaseWords = wordsWithStopWords.filter(!broadcastStopWords.value.contains(_))
      .filter(x => x.length > 1).filter(x => !x.matches("\\d+")).map(x => x.toLowerCase())

    // Count of the occurrences of each word
    val wordCounts = lowercaseWords.map(x => (x, 1)).reduceByKey((x,y) => x + y)

    // Flip (word, count) tuples to (count, word) and then sort by key (the counts)
    val wordCountsSorted = wordCounts.map(x => (x._2, x._1)).sortByKey()

    // Print the results, flipping the (count, word) results to word: count as we go.
    for (result <- wordCountsSorted) {
      val count = result._1
      val word = result._2
      println(s"$word: $count")
    }

  }

}