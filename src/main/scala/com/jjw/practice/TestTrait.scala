package com.jjw.practice


/**
  * trait 相当于java中的抽象类和接口
  */
trait Listen {

  val tp = "listen"

  def listen(actio: String) = {
    println(actio + "is Listenning...")
  }
}

trait Speak {
  val tp = "speak"

  def speak(actio: String) = {
    println(actio + "is Speaking...")
  }
}


/**
  * scala中多继承时，第一个关键字用extends,以后无论继承多少个trait都用with关键字
  */
class Person1 extends Listen with Speak {
  override val tp: String = "person" // 覆盖


}

object TestTrait {
  def main(args: Array[String]): Unit = {
    val p = new Person1()
    println(p.tp)
    p.listen("听...")
    p.speak("说...")

  }
}