package com.mao.streaming

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * java -Xms256m -Xmx1024m -jar ./WindowCounter.jar spark://192.168.2.2:8888 192.168.2.2 9999 5 30 30
  * @author bigdope
  * @ create 2018-09-20
  **/
object WindowCounter {

  def main(args: Array[String]): Unit = {

    if (args.length < 4) {
      System.err.println("Usage: WindowCounter <master> <hostname> <port> <interval> " +
        "<windowLength> <slideInterval> \n " +
        "In local mode, <master> should be 'local[n]' with n > 1")
      System.exit(1)
    }

    //创建StreamingContext
    val ssc = new StreamingContext(args(0), "WindowCounter", Seconds(args(3).toInt),
      "/home/hadoop/app/hadoop/spark-2.1.0-bin-hadoop2.4", StreamingContext.jarOfClass(this.getClass).toSeq)

    ssc.checkpoint(".")

    val lines = ssc.socketTextStream(args(1), args(2).toInt, StorageLevel.MEMORY_ONLY_SER)
    val words = lines.flatMap(_.split(" "))

//    val wordCounts = words.map(x => (x, 1)).reduceByKeyAndWindow((x : Int, y : Int) => x + y, Seconds(args(4).toInt), Seconds(args(5).toInt))
    val wordCounts = words.map(x => (x, 1)).reduceByKeyAndWindow(_+_, _-_, Seconds(args(4).toInt), Seconds(args(5).toInt))

    val sortedWordCount = wordCounts.map{
      case (char, count) => (count, char)
    }.transform(_.sortByKey(false)).map{
      case (char, count) => (count, char)
    }

    wordCounts.print()
    sortedWordCount.print()
    ssc.start()
    ssc.awaitTermination()

  }

}
