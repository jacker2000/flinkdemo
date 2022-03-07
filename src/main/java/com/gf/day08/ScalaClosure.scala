package com.gf.day08

/**
 *  闭包
 */
object ScalaClosure {

  def add(i: Int): Int=>Int = {
    println(i)
    def help(m:Int):Int={
      println("i:"+i)
      println("m:"+m)
      m+i
    }
    help
  }

  def main(args: Array[String]): Unit = {
    val addOne: Int => Int = add(1)
    println(addOne(2))
  }

}
