package com.jjw.practice


/**
  * 注意
  * 1. object 相当于java单例对象，object中定义的是静态东西
  * class类中不定义main方法也运行不了，因为main方法是静态的，class中不让定义静态方法
  * 2. scala中一行后面可以省略分号，会有分号自动推断。如果多个语句会写在一行中不能省略
  * 3. val 定义常量,常量不可变 var 定义变量，变量可变。 声明的变量可以自动推断，也可以显示声明。注意：递归和高级函数()需要声明数据类型
  * 4. scala中同一个包下的所有scala文件相当于写在一个大文件中，所以同一个包下不允许有重名的类名。同一个包下有报错的类，即便没问题的代码也
  * 不允许运行(相当于一个文件)
  * 5. scala创建类可以传参如果传参相当于有了默认的构造函数，Object创建不可以传参 （java中类不允许传参的）
  * scala中默认有getter、setter 方法
  * 6. scala声明变量和常量默认访问级别相当于java和public
  * 7. 如果同一个scala文件中object的名称和class的名称相同，那么这个类叫做这个对象的伴生类，这个对象叫做这个类的伴生对象。彼此之间可以相互
  * 调用private方法和变量
  * 8. 当new一个类的时候，类中除了方法不执行，其他都执行
  */
class Person {

}

class User(pname: String, page: Int) {

  var name = pname
  var age = pname
  var sex = "男"
  println(pname + "---" + page)

  // 重写默认构造函数
  def this(yname: String, yage: Int, ysex: String) = {
    this(yname, yage) // 第一行一定要引用默认构造方法
    this.sex = ysex

  }
}

object Test01 {

  def main(args: Array[String]): Unit = {
    println("abc")

    val a = 100
    var a1: Int = 100 // 手动知道a1的数据类型，不写自定推断为Int
    var b = 100
    b = a

    val user = new User("张三", 18)


    // for循环(to 包含最后一个数， until不包含最后一个数  后面是步长)
    println(1 to(10, 2))
    println(1 until 10)
    println(1 until(10, 3))

    // 1 - 10
    for (i <- 1 to 10) {
      println(i)
    }

    println("打印乘法口诀")

    for (i <- 1 until 10) {
      for (j <- 1 until (10)) {
        if (i >= j) {
          print(j + "*" + i + "=" + i * j + "\t")
        }
        if (i == j) {
          println()
        }
      }
    }

    // 双层for循环可以如下
    for (i <- 1 to 10; j <- 1 to 10) {

    }
  }
}
