package com.mao.streaming

import java.io.PrintWriter
import java.net.ServerSocket

import scala.collection.mutable.ListBuffer

/**
  *  finalName: LoggerSimulation
  *  java -jar ./LoggerSimulation.jar 9999 500
  * 模拟随记发送A-G 7个字母中的一个，时间间隔可以指定
  *
  *  @author bigdope
  * @ create 2018-09-17
  **/
class LoggerSimulation {

}

object LoggerSimulation {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: <port> <millisecond>")
    }

    val listener = new ServerSocket(args(0).toInt)
    while (true) {
      val socket = listener.accept()
      new Thread() {
        override def run(): Unit = {
          println("Go client connected from:" + socket.getInetAddress)
          val out = new PrintWriter(socket.getOutputStream(), true)
          while (true) {
            Thread.sleep(args(1).toLong)
            val content = generateContent(index)
            println(content)
            out.write(content + "\n")
            out.flush()
          }
          socket.close()
        }
      }.start()
    }

  }

  def generateContent(index: Int) : String = {

    val  charList = ListBuffer[Char]()
    for (i <- 65 to 90) {
      charList += i.toChar
    }
    val charArray = charList.toArray
    charArray(index).toString
  }

  def index = {
    import java.util.Random
    val random = new Random()
    random.nextInt(7)
  }


}
