package com.mao.spark

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * java -Xms256m -Xmx1024m -jar ./scala-maven-demo-1.0-SNAPSHOT.jar ./scala-maven-demo-1.0-SNAPSHOT.jar hdfs://192.168.2.2:9000/spark/article.data hdfs://192.168.2.2:9000/spark/articleResult
  * java -Xms256m -Xmx1024m -jar ./scala-maven-demo-1.0-SNAPSHOT.jar ./scala-maven-demo-1.0-SNAPSHOT.jar file:////home/hadoop/testfiles/spark//article.data file:///home/hadoop/testfiles/spark/articleResult
  * http://192.168.2.2:8080/
  *  @author bigdope
  * @ create 2018-09-14
  **/
class Analysis {

}

object Analysis {

  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      println("Usage: java -jar code.jar dependency_jars file_location save location")
      System.exit(0)
    }

    val jars = ListBuffer[String]()
    args(0).split(',').map(jars += _)

    val conf = new SparkConf()
    conf.setMaster("spark://192.168.2.2:8888")
        .setSparkHome("/home/hadoop/app/hadoop/spark-2.1.0-bin-hadoop2.4")
      .setAppName("analysis")
      .setJars(jars)
      .set("spark.executor.memory", "1g")

    val sc = new SparkContext(conf)
    val data = sc.textFile(args(1))

    data.cache()

    println(data.count())

    data.filter(_.split(' ').length == 3).map(_.split(' ')(1)).map((_, 1)).reduceByKey(_+_)
      .map(x => (x._2, x._1)).sortByKey(false).map(x => (x._2, x._1)).saveAsTextFile(args(2))

  }

}
