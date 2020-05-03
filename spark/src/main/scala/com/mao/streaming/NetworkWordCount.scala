package com.mao.streaming

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Durations, Seconds, StreamingContext}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer

/**
  * finalName: NetworkWordCount
  * java -jar ./NetworkWordCount.jar spark://192.168.2.2:8888 localhost 9999 10
  * java -Xms256m -Xmx1024m -jar ./NetworkWordCount.jar spark://192.168.2.2:8888 localhost 9999 10 NetworkWordCount.jar
  *
  * https://blog.csdn.net/mrmiantuo/article/details/43565395
  * https://blog.csdn.net/qq_29651795/article/details/70145183
  * https://blog.csdn.net/SunnyYoona/article/details/59124645
  *
  * @author bigdope
  * @ create 2018-09-17
  **/
object NetworkWordCount {

//  protected final val logger : Logger= LoggerFactory.getLogger(this.getClass())

  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      System.err.println("Usage: NetworkWordCount <master> <hostname> <port> <seconds> \n" +
        "In local mode, <master> should be 'local[n]' with n > 1")
      System.exit(1)
    }

    //插入日志
//    logger.info("handline file:{}",f.getPath)

    //创建StreamingContext
//    val ssc = new StreamingContext(args(0), "NetworkWordCount", Seconds(args(3).toInt),
//      System.getenv("SPARK_HOME"), StreamingContext.jarOfClass(this.getClass).toSeq)

    val ssc = new StreamingContext(args(0), "NetworkWordCount", Seconds(args(3).toInt),
          "/home/hadoop/app/hadoop/spark-2.1.0-bin-hadoop2.4", StreamingContext.jarOfClass(this.getClass).toSeq)

//    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")

    val jars = ListBuffer[String]()
    args(4).split(',').map(jars += _)

//    val conf = new SparkConf().setMaster(args(0))
//      .setAppName("NetworkWordCount")
//      .setSparkHome("/home/hadoop/app/hadoop/spark-2.1.0-bin-hadoop2.4")
//      .setJars(jars)
////      .setJars(StreamingContext.jarOfClass(this.getClass).toList)
//    val ssc = new StreamingContext(conf, Seconds(args(3).toInt))
////    val ssc = new StreamingContext(conf, Durations.seconds(1))

    //创建NetworkInputDStream，需要指定ip和端口
    val lines = ssc.socketTextStream(args(1), args(2).toInt, StorageLevel.MEMORY_ONLY_SER)
    val words = lines.flatMap(_.split(" "))
    val wordCount = words.map(x => (x, 1)).reduceByKey(_+_)
    wordCount.print()
    ssc.start()
    ssc.awaitTermination()




  }

}
