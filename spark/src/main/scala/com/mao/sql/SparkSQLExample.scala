package com.mao.sql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * java -Xms256m -Xmx1024m -jar SparkSQLExample.jar com.mao.sql.SparkSQLExample
  * ./run-example org.apache.spark.examples.sql.SparkSQLExample
  * @author bigdope
  * @ create 2018-09-25
  **/
object SparkSQLExample {

  //定义匹配类
  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {

//    //创建sparkSeesion
//    val spark = SparkSession
//      .builder()
//      .appName("Spark SQL basic example")
//      .config("spark.some.config.option", "some-value")
//      .getOrCreate()

//    //创建sparkSeesion
//    val spark = SparkSession
//      .builder()
//      .appName("Spark SQL basic example")
//      .config("master", "spark://192.168.2.2:8888")
//      .getOrCreate()

    //创建sparkSeesion
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .master("spark://192.168.2.2:8888")
      .getOrCreate()

    //导入隐式转换
    import spark.implicits._

    runBasicDataFrameExample(spark)

    runDatasetCreationExample(spark)

    runInferSchemaExample(spark)

    runProgrammaticSchemaExample(spark)

    spark.stop()

  }

  /*
   *基本的使用
   */
  private def runBasicDataFrameExample(spark: SparkSession): Unit = {
    // 读取一个json数据格式的问价
//    val df = spark.read.json("/people.json")
//    val df = spark.read.json("examples/src/main/resources/people.json") //相当于这个路径 file:/home/hadoop/jar/sparkJar/examples/src/main/resources/people.json
    val df = spark.read.json("hdfs://192.168.2.2:9000/user/root/examples/src/main/resources/people.json")

    //输出数据
    df.show()
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+

    //导入隐式转换
    import spark.implicits._
    //输出json文件的约束
    df.printSchema()
    // root
    // |-- age: long (nullable = true)
    // |-- name: string (nullable = true)

    // 只显示name列
    df.select("name").show()
    // +-------+
    // |   name|
    // +-------+
    // |Michael|
    // |   Andy|
    // | Justin|
    // +-------+

    // 查找name 和age+1
    df.select($"name", $"age" + 1).show()
    // +-------+---------+
    // |   name|(age + 1)|
    // +-------+---------+
    // |Michael|     null|
    // |   Andy|       31|
    // | Justin|       20|
    // +-------+---------+

    // 筛选出年龄大于21的
    df.filter($"age" > 21).show()
    // +---+----+
    // |age|name|
    // +---+----+
    // | 30|Andy|
    // +---+----+

    // 用年龄进行分组
    df.groupBy("age").count().show()
    // +----+-----+
    // | age|count|
    // +----+-----+
    // |  19|    1|
    // |null|    1|
    // |  30|    1|
    // +----+-----+

    //创建一个临时的people表
    df.createOrReplaceTempView("people")

    //执行sql语句
    val sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+

  }

  /*
   *DataSet的使用
   */
  private def runDatasetCreationExample(spark: SparkSession): Unit = {
    import spark.implicits._

    //创建一个people然后转化为DataSet
    val caseClassDS = Seq(Person("Andy", 32)).toDS()
    caseClassDS.show()
    // +----+---+
    // |name|age|
    // +----+---+
    // |Andy| 32|
    // +----+---+

    //报错
    //Caused by: java.lang.ClassCastException: cannot assign instance of scala.collection.immutable.List$SerializationProxy to field org.apache.spark.rdd.RDD.org$apache$spark$rdd$RDD$$dependencies_ of type scala.col
    //lection.Seq in instance of org.apache.spark.rdd.MapPartitionsRDD
//    val primitiveDS = Seq(1, 2, 3).toDS()
//    primitiveDS.map(_ + 1).collect()  // Returns: Array(2, 3, 4)

//    val path = "examples/src/main/resources/people.json"
    val path = "hdfs://192.168.2.2:9000/user/root/examples/src/main/resources/people.json"
    val peopleDS = spark.read.json(path).as[Person]
    peopleDS.show()
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+

  }

  /*
  *带有约束的表（带有字段的表）
  */
  private def runInferSchemaExample(spark: SparkSession): Unit = {
    import spark.implicits._

    // 创建一个RDD 然后转化成dataframe
//    val peopleDF = spark.sparkContext
//      .textFile("examples/src/main/resources/people.txt")
//      .map(_.split(","))
//      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
//      .toDF()

    val peopleDF = spark.sparkContext
      .textFile("hdfs://192.168.2.2:9000/user/root/examples/src/main/resources/people.txt")
      .map(_.split(","))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
      .toDF()

    //创建一个临时的people表
    peopleDF.createOrReplaceTempView("people")

    //查询
    val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")

    //报错
    //Caused by: java.lang.ClassCastException: cannot assign instance of scala.collection.immutable.List$SerializationProxy to field org.apache.spark.rdd.RDD.org$apache$spark$rdd$RDD$$dependencies_ of type scala.col
    //lection.Seq in instance of org.apache.spark.rdd.MapPartitionsRDD
    //通过下标查数据
    teenagersDF.map(teenager => "Name: " + teenager(0)).show()
    // +------------+
    // |       value|
    // +------------+
    // |Name: Justin|
    // +------------+

    // 通过field name查数据
    teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()
    // +------------+
    // |       value|
    // +------------+
    // |Name: Justin|
    // +------------+

    //这点据说还很重要，但是不是太理解
    // No pre-defined encoders for Dataset[Map[K,V]], define explicitly
    implicit  val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
    //也可以写成下面这个样子
    // implicit val stringIntMapEncoder: Encoder[Map[String, Any]] = ExpressionEncoder()

    // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
    teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()
    // Array(Map("name" -> "Justin", "age" -> 19))
  }

  private def runProgrammaticSchemaExample(spark: SparkSession): Unit = {
    import spark.implicits._

    //创建一个 RDD
//    val peopleRDD = spark.sparkContext.textFile("examples/src/main/resources/people.txt")
    val peopleRDD = spark.sparkContext.textFile("hdfs://192.168.2.2:9000/user/root/examples/src/main/resources/people.txt")

    // The schema is encoded in a string
    val schemaString = "name age"

    // 把一个字符串数组转化成一个fields
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))

    val schema = StructType(fields)

    // Convert records of the RDD (people) to Rows
    val rowRDD = peopleRDD
      .map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1).trim))

    //把这个约束应用到这个RDD上
    val peopleDF = spark.createDataFrame(rowRDD, schema)

    peopleDF.createOrReplaceTempView("people")

    val results = spark.sql("SELECT name FROM people")

    results.map(attributes => "Name: " + attributes(0)).show()
    // +-------------+
    // |        value|
    // +-------------+
    // |Name: Michael|
    // |   Name: Andy|
    // | Name: Justin|
    // +-------------+

  }


}
