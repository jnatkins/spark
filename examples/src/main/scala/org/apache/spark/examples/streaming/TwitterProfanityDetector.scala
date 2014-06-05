/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples.streaming

import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import StreamingContext._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf

/**
 * Calculates popular hashtags (topics) over sliding 10 and 60 second windows from a Twitter
 * stream. The stream is instantiated with credentials and optionally filters supplied by the
 * command line arguments.
 *
 * Run this on your local machine as
 *
 */
object TwitterProfanityDetector {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: TwitterProfanityDetector <consumer key> <consumer secret> " +
        "<access token> <access token secret> [<filters>]")
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generat OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sparkConf = new SparkConf().setAppName("TwitterProfanityDetector")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint(".")
    val stream = TwitterUtils.createStream(ssc, None, filters)

    // Pull the blacklist into an RDD for reuse with each batch
    val blacklist = ssc.sparkContext.textFile("blacklist.txt")
                       .flatMap(line => line.split(","))
                       .map(word => (word, 1))
                       .persist()

    // The flow here is to:
    // 1. Split each status into words
    // 2. Associate each word with the status it came from
    // 3. Join from the blacklist to the words (this discards any non-profane tweets)
    // 4. Pull out the status IDs of any profane tweets
    // 5. Identify the distinct status IDs
    val profaneTweets = stream.flatMap(status => status.getText.split(" ")
                              .map(word => (word, status.getId)))
                              .transform(rdd => rdd.join(blacklist)
                                                   .map{case (word, id) => id._1}
                                                   .distinct)

    val profaneTweets1 = profaneTweets.countByWindow(Minutes(1), Minutes(1))
    val totalTweets1 = stream.countByWindow(Minutes(1), Minutes(1))

    val profaneTweets5 = profaneTweets.countByWindow(Minutes(5), Minutes(1))
    val totalTweets5 = stream.countByWindow(Minutes(5), Minutes(1))

    val profaneTweets60 = profaneTweets.countByWindow(Minutes(60), Minutes(1))
    val totalTweets60 = stream.countByWindow(Minutes(60), Minutes(1))

    totalTweets1.foreachRDD(rdd => {
      val tweetCount = rdd.take(1)
      tweetCount.foreach{case (count) => println("\nTotal tweets in last minute: %d".format(count))}
    })

    profaneTweets1.foreachRDD(rdd => {
      val tweetCount = rdd.take(1)
      tweetCount.foreach{case (count) => println("\nProfane tweets in last minute: %d".format(count))}
    })

    totalTweets5.foreachRDD(rdd => {
      val tweetCount = rdd.take(1)
      tweetCount.foreach{case (count) => println("\nTotal tweets in last 5 minutes: %d".format(count))}
    })

    profaneTweets5.foreachRDD(rdd => {
      val tweetCount = rdd.take(1)
      tweetCount.foreach{case (count) => println("\nProfane tweets in last 5 minutes: %d".format(count))}
    })

    totalTweets60.foreachRDD(rdd => {
      val tweetCount = rdd.take(1)
      tweetCount.foreach{case (count) => println("\nTotal tweets in last hour: %d".format(count))}
    })

    profaneTweets60.foreachRDD(rdd => {
      val tweetCount = rdd.take(1)
      tweetCount.foreach{case (count) => println("\nProfane tweets in last hour: %d".format(count))}
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
