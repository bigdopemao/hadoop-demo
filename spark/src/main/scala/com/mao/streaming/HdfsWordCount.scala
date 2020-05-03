package com.mao.streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *  java -Xms256m -Xmx1024m -jar ./HdfsWordCount.jar spark://192.168.2.2:8888 hdfs://192.168.2.2:9000/spark/testFile 10
  *  @author bigdope
  * @ create 2018-09-20
  **/
object HdfsWordCount {

  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      System.err.println("Usage: HdfsWordCount <master> <directory> <seconds> \n" +
        "In local mode, <master> should be 'local[n]' with n > 1")
      System.exit(1)
    }

    //建创StreamingContext
    val ssc = new StreamingContext(args(0), "HdfsWordCount", Seconds(args(2).toInt),
    "/home/hadoop/app/hadoop/spark-2.1.0-bin-hadoop2.4", StreamingContext.jarOfClass(this.getClass).toSeq)

    //创建FileInputDStream, 并指向特定目录
    val lines = ssc.textFileStream(args(1))
    val words = lines.flatMap(_.split(" "))
    val wordCount = words.map(x => (x, 1)).reduceByKey(_+_)
    wordCount.print()
    ssc.start()
    ssc.awaitTermination()

  }

}
