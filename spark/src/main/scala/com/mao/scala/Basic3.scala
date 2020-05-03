package com.mao.scala

/**
  * @author bigdope
  * @ create 2018-09-11
  **/
class Basic3 {

}

//abstract class Person1 {
//  def speak
//  var name : String
//  var age : Int
//}
//
//class Student1 extends Person1 {
////  def speak: Unit = {
////    println("speak！！！")
////  }
//  def speak {
//    println("speak！！！")
//  }
//  var name = "Jack"
//  var age = 100
//
//}

//trait Logger {
//  def log(msg : String) {
//    println("log " + msg)
//  }
//}
//
//class TestLogger extends Logger {
//  def testLog() {
//    log("xxx")
//  }
//}

//trait Logger {
//  def log(msg : String)
//}
//
//trait ConsoleLogger extends Logger {
//  def log(msg : String) {
//    println(msg)
//  }
//}
//
//class  TestLogger extends ConsoleLogger {
//  def test {
//    log("log")
//  }
//}

trait ConsoleLogger {
  def log(msg : String): Unit = {
    println("save money" + msg)
  }
}

trait MessageLogger extends ConsoleLogger{
  override def log(msg: String): Unit = {
    println("save money to bank" + msg)
  }
}

abstract class Account {
  def save
}

class MyAccount extends Account with ConsoleLogger {
  def save: Unit = {
    log("100")
  }
}

object Basic3 extends App {

//  var s = new Student1
//  s.speak
//  println(s.name + " : " + s.age)

//  val t = new TestLogger
//  t.testLog()

//  val acc = new MyAccount
//  acc.save

  val acc = new MyAccount with MessageLogger
  acc.save
}
