package com.andrewrgoss.spark

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._

/** Listens to a stream of Tweets and keeps track of the most common
 *  words associated with data over a 7 minute window.
 */
object DataChatter {
  
    /** Makes sure only ERROR messages get logged to avoid log spam. */
  def setupLogging() = {
    import org.apache.log4j.{Level, Logger}   
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)   
  }
  
  /** Configures Twitter service credentials using twitter.txt in the main workspace directory */
  def setupTwitter() = {
    import scala.io.Source
    
    for (line <- Source.fromFile("../twitter_streaming/twitter.txt").getLines) {
      val fields = line.split(" ")
      if (fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
  }
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Configure Twitter credentials using twitter.txt
    setupTwitter()

    // Create a SparkContext using all CPU cores of the local machine
    val sc = new SparkContext("local[*]", "DataChatter")
    
    // Set up a Spark streaming context named "DataChatter" that runs locally using
    // all CPU cores and two-second batches of data
    val ssc = new StreamingContext(sc, Seconds(2))

    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    // Create RDD for stop words/symbols to omit from tweets
    val stopWordsInput = sc.textFile("../twitter_streaming/data/stop-word-symbol-list.csv") // Stop words & symbols

    // Flatten, collect, and broadcast stop words/symbols
    val stopWords = stopWordsInput.flatMap(x => x.split(",")).map(_.trim) // Flatmap transforms an RDD of length N
    // into a collection of N collections, then flattens these into a single RDD of results
    val broadcastStopWords = sc.broadcast(stopWords.collect.toSet) // Keep read-only variable cached on local machine

    // Create a DStream from Twitter using our streaming context with filters for "data"
    val tweets = TwitterUtils.createStream(ssc, None, Array("data"))
    
    // Extract the text of each status update into DStreams using map()
    val statuses = tweets.map(status => status.getText())
    
    // Blow out each word into a new DStream and convert to lowercase
    val tweetwords = statuses.flatMap(tweetText => tweetText.split(" ").map(tweetText => tweetText.toLowerCase()))
    
    // Now eliminate anything that's not a stop word as well as single character values
    // Leave meaningful words associated with data (excluding "data" itself)
    val dataChatter = tweetwords.filter(!broadcastStopWords.value.contains(_)).filter(x => x.length > 1).filter(x => !x.matches("data"))
    
    // Map each tweetword to a key/value pair of (tweetword, 1) so we can count them up by adding up the values
    val dataChatterKeyValues = dataChatter.map(tweetword => (tweetword, 1))
    
    // Now count them up over a 7 minute window sliding every two seconds
    val dataChatterCounts = dataChatterKeyValues.reduceByKeyAndWindow( _ + _, _ -_, Seconds(420), Seconds(2))
    
    // Sort the results by the count values
    val sortedResults = dataChatterCounts.transform(rdd => rdd.sortBy(x => x._2, false))
    
    // Print the top 10
    sortedResults.print
    
    // Set a checkpoint directory, and kick it all off
    // This will run until terminated
    ssc.checkpoint("../twitter_streaming/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }  
}
