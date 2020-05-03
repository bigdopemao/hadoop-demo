package com.mao.scala

/**
  * @author bigdope
  * @ create 2018-09-12
  **/
class Basic6 {

}

class A {

}

class RichA(a : A) {
  def rich: Unit = {
    println("rich ...")
  }
}

object Basic6 extends App {

  implicit def a2RichA(a : A) = new RichA(a)

  val a = new A
  a.rich

  def testParam(implicit name : String): Unit = {
    println(name)
  }

  implicit val name = "implicit !!!"

  testParam
  testParam("hhh")

  implicit class Calculate(x : Int) {
    def add(a : Int) : Int = a + 1
  }

  println(1.add(1))
  println(2.add(3))

}
