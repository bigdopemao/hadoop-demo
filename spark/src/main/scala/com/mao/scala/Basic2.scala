package com.mao.scala

/**
  * @author bigdope
  * @ create 2018-09-11
  **/
class Basic2 {

}

//class Person {
//
//  var name : String = _ //会生成getter和setter方法
//  val age = 10 //只会生成getter方法
//  private[this] val gender = "male"
//
//}

//1.主构造器直接跟在类名后面，主构造器中的参数，最后会被编译成字段
//2.主构造器执行的时候，会执行类中的所有语句
//3.假如参数声明时不带val或var，那相当于private[this] !!!
class Person(val name : String, val age : Int) {
  println("this is the primary constructor")

  var gender : String = _
  val school = "HHH"

  //1.附属构造器的名称为this
  //2.每一个附属构造器必须先调用已经存在的自构造器或者附属构造器
  def this(name : String, age : Int, gender : String) {
    this(name, age)
    this.gender = gender
  }

}

class Student(name : String, age : Int, val major : String) extends Person(name, age) {
  println("this is the subclass of Person, major is : " + major)

//  override def toString = {"override toString"}
  override def toString = "override toString"

//  override val school: String = "hhh"
  override val school = "hhh"
}


object Basic2 {

  def main(args : Array[String]): Unit = {
//    val p = new Person //括号可以省略
//    p.name = "Jack"
//    println(p.name + " : " + p.age)

//    val p = new Person("jack", 20)
//    println(p.name + " : " + p.age)

//    val p = new Person("jack", 20, "male")
//    println(p.name + " : " + p.age + " : " + p.gender)

    val s = new Student("jack", 20, "Math")
    s.gender = "aa"
    println(s.gender)
    println(s.toString())

  }

}
