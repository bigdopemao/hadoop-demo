package com.mao.scala

/**
  * @author bigdope
  * @ create 2018-09-12
  **/
class Basic4 {

}

class ApplyTest {
  def apply() = "APPLY"
  def test: Unit = {
    println("test")
  }
}

object ApplyTest {
  def apply(): ApplyTest = new ApplyTest()
  def static: Unit = {
    println("i'm a static method")
  }

  var count = 0
  def incr = {
    count = count + 1
  }
}

object Basic4 extends App {
//  ApplyTest.static

//  val a = ApplyTest()
//  a.test

//  val t = new ApplyTest
//  println(t())
//  println(t)

  for (i <- 1 to 10) {
    ApplyTest.incr
  }
  println(ApplyTest.count)

}
