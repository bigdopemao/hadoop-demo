package com.mao.scala

/**
  * @author bigdope
  * @ create 2018-09-12
  **/
class Basic5 {

}

case class Book(name : String, author : String)

object Basic5 extends App {

  val value = 3

  var result = value match {
    case 1 => "one"
    case 2 => "two"
    case _ => "some other number"
  }

  var result2 = value match {
    case i if i == 1 => "one"
    case i if i == 2 => "two"
    case _ => "some other number"
  }

  println("result of match is: " + result)
  println("result2 of match is: " + result2)

  def t(obj : Any) = obj match {
    case i : Int => println("Int")
    case i : String => println("String")
    case _ => println("unknown type")
  }

  t(1)

  val macTalk = Book("macTalk", "hhh")
  macTalk match {
    case Book(name, author) => println("this is a book")
    case _ => println("unknown")
  }

}
