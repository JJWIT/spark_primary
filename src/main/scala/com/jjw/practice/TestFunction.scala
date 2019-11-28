package com.jjw.practice

import java.util.Date

object TestFunction {

  def main(args: Array[String]): Unit = {


  }

  /**
    * 1. 函数的定义：
    * 声明函数，函数会把最后一行的计算结果当做返回值，如果使用return 则函数返回值类型必须显示声明
    * 声明函数时参数类型不能省略
    *
    * @param a
    * @param b
    */

  def max(a: Int, b: Int): Int = {
    if (a > b) {
      a
    } else {
      b
    }
  }

  println(max(10, 20))

  /**
    * 2. 递归函数
    */

  def fun(num: Int): Int = {

    if (num == 1) {
      1
    } else {
      num * fun(num - 1)
    }
  }

  println(fun(5))

  /**
    * 3. 有默认值的函数声明
    * 如果想要一个函数有返回值必须写=，如果省略=，无论最后一行计算结果是什么都会被丢弃
    */

  def fun(a: Int, b: Int): Int = {
    a + b;
  }

  println(fun(1, 2))


  def fun1(a: Int = 100, b: Int = 200): Int = {
    a + b;
  }

  println(fun1()) //使用默认值100、200
  println(fun1(1)) // 使用b的默认值
  println(fun1(b = 1)) // 指定b为1
  println(fun1(1, 2))


  /**
    * 4. 多个同类型参数的函数声明
    *
    */

  def fun(a: Int*): Unit = {
    for (i <- a) {
      println(i)
    }
  }

  fun(1, 2, 5, 4)

  /**
    * 5. 匿名函数声明 (java里面有匿名内部类)
    *
    * 在scala中"=>"代表匿名内部函数
    */

  // 5.1 无参匿名内部函数, 函数返回值给fun变量
  val fun = () => {
    println("hello world...")
  }
  fun()

  // 5.2 有参匿名内部函数, 函数返回值给fun1变量
  val fun1 = (content: String) => {
    println(content)
  }

  fun1("中国...")

  /**
    * 6.嵌套函数
    */

  def fun6(num: Int) = {
    def fun(a: Int): Int = {
      if (a == 1) {
        1
      } else {
        0
      }
    }

    fun(num) // 注意需要在方法内调用嵌套函数
  }


  println("6.嵌套函数返回值，result = " + fun6(5))


  /**
    * 7. 偏应用函数：是一个表达式，只需要提供部分参数，不需要提供所有参数
    *
    */

  println("7. 偏应用函数...")

  def showLog(date: Date, log: String) = {
    println("date is " + date + ", log is " + log)
  }

  val date = new Date()
  showLog(date, "日志1")
  showLog(date, "日志2")

  // 改成偏应用函数
  val fun7 = showLog(date, _: String)
  fun7("日志1...")
  fun7("日志2...")
  fun7("日志3...")

  /**
    * 8. 高阶函数
    * 8.1 函数的参数是函数
    * 8.2 函数的返回值是函数
    * 8.3 函数的参数和返回值都是参数
    */

  // 8.1 函数的参数是函数

  def fun8(a: Int, b: Int): Int = {
    a + b
  }

  def fun81(f: (Int, Int) => Int): Int = { // 函数的参数是函数， 参数是fun8
    f(1, 2)
  }

  println("高阶函数，函数的参数是函数, result = " + fun81(fun8))

  // 8.2 函数的返回值是函数
  def fun82(a: Int, b: Int): (Int, Int) => String = { // 函数的返回值是函数
    def fun8(p1: Int, p2: Int): String = {
      "{ a = " + a + ", b = " + b + ", p1 = " + p1 + ", p2 = " + p2 + " }"
    }

    return fun8
  }

  println("8.2 高阶函数， 函数的返回值是函数， result = " + fun82(1, 2))
  println("8.2 高阶函数， 函数的返回值是函数， result = " + fun82(1, 2)(3, 4))

  // scala 第05个视频28分钟


  // 字符串
  println("****** 字符串 ******")
  val sb = new StringBuilder
  sb.+('a')
  sb.++=("bcde")
  println(sb)
  sb.append("ddddd")
  println(sb)

  // 数组
  println("****** 数组 ******")
  val arr = Array(1, 2, 3, 4, 5f) // scala2.0 以上可以混写
  val arr1 = Array[Int](1, 2, 4, 3) // 指定泛型

  for (elem <- arr) {
    println(elem)
  }

  arr.foreach(x => {
    println(x)
  })
  arr1.foreach(println(_))


  val arr2 = new Array[Int](3) // new 数组需要添加数组长度
  arr2(0) = 111
  arr2(1) = 222
  arr2(2) = 333
  arr2.foreach(println(_))

  println("****** concat ******")
  val arr3 = Array(1, 2)
  val arr4 = Array(3, 4)
  val arr5 = Array.concat(arr3, arr4)
  arr5.foreach(println(_))

  println("****** fill ******")
  val arr6 = Array.fill(5)("abc") // fill 填充
  arr6.foreach(println((_)))


  println("****** tuple ******")
  val tuple1 = new Tuple1(1)
  val tuple2 = new Tuple2(1, 2)
  val tuple3 = Tuple3(1, 2, 3)
  val tuple4 = (1, 2, 3, 4)
  println(tuple4._2) // 取第2个

  val iterator = tuple4.productIterator
  while (iterator.hasNext) {
    println(iterator.next())
  }

  // list
  println("****** list ******")
  val list = List("node1", "node2", "node3")
  val list1 = List[Int](1, 2, 3, 4)
  list.foreach(println)
  for (elem <- list1) {
    println(elem)
  }


}
