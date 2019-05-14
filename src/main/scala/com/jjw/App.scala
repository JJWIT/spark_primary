package com.jjw

import org.spark_project.dmg.pmml.Application

/**
 * Hello world!
 *
 */
object App extends Application {
  println( "Hello World!" )

  def main(args: Array[String]): Unit = {
    val a = "abc"
    println(a)
  }

  def method (args : Array[String]) : Unit = {
    val b = "abd"
    println(b)
  }
}
