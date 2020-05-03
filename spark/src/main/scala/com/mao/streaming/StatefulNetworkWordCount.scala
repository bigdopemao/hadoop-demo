package com.mao.streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * java -Xms256m -Xmx1024m -jar ./StatefulNetworkWordCount.jar spark://192.168.2.2:8888 localhost 9999 10
  * @author bigdope
  * @ create 2018-09-19
  **/
object StatefulNetworkWordCount {

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println("Usage: StatefulNetworkWordCount <master> <hostname> <port> <seconds> \n" +
        "In local mode, <master> should be 'local[n]' with n > 1")
      System.exit(1)
    }


    //创建StreamingContext
    val ssc = new StreamingContext(args(0), "StatefulNetworkWordCount",
      Seconds(args(3).toInt), "/home/hadoop/app/hadoop/spark-2.1.0-bin-hadoop2.4", StreamingContext.jarOfClass(this.getClass).toSeq);
    ssc.checkpoint(".")

    //创建NetworkInputDStream，需要指定ip和端口
    val lines = ssc.socketTextStream(args(1), args(2).toInt)
    val words = lines.flatMap(_.split(" "))
    val wordDstream = words.map(x => (x, 1))

    val updateFunc = (values : Seq[Int], state : Option[Int]) => {
      val currentCount = values.foldLeft(0)(_+_)
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }

    //使用updateStateByKey来更新状态
    val stateDstream = wordDstream.updateStateByKey[Int](updateFunc)
    stateDstream.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
