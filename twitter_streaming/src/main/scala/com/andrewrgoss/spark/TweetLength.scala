package com.andrewrgoss.spark

import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._

/** Listens to a stream of Tweets and keeps track of the most common
 *  tweet length associated with data over a 5 minute window.
 */
object TweetLength {
  
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

    // Set up a Spark streaming context named "TweetLength" that runs locally using
    // all CPU cores and one-second batches of data
    val ssc = new StreamingContext("local[*]", "PopularHashtags", Seconds(1))

    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    // Create a DStream from Twitter using our streaming context with filters for "data"
    val tweets = TwitterUtils.createStream(ssc, None, Array("data"))
    
    // Extract the text of each status update into DStreams using map()
    val statuses = tweets.map(status => status.getText())
    
    // Store the length of each status update into a new DStream
    val statusLength = statuses.map(x => x.size)

    // Map each statusLength to a key/value pair of (statusLength, 1) so we can count them up by adding up the values
    val statusLengthKeyValues = statusLength.map(tweetLength => (tweetLength, 1))

    // Now count them up over a 5 minute window sliding every one second
    val statusLengthCounts = statusLengthKeyValues.reduceByKeyAndWindow( _ + _, _ -_, Seconds(300), Seconds(1))

    // Sort the results by the count values
    val sortedResults = statusLengthCounts.transform(rdd => rdd.sortBy(x => x._2, false))

    // Print the top 10 most common tweet lengths (number of characters)
    sortedResults.print
    
    // Set a checkpoint directory, and kick it all off
    // This will run until terminated
    ssc.checkpoint("../twitter_streaming/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }  
}
