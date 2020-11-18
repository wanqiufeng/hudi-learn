package com.niceshot.hudi

import scala.util.control.Breaks.{break, breakable}

/**
 * @author created by chenjun at 2020-11-18 10:46
 *
 */
object TestLoop {
  def main(args: Array[String]): Unit = {
    //break example
    breakable {
      for(i <- 1 to 3) {
        if(i==1) {
          println("tiao出了")
          break
        }
      }
    }

  //continue example
  for(i <- 1 to 3) {
    breakable {
      if(i == 1) {
        println("本次跳出")
        break
      }
      println("其余的还会继续执行")
    }
  }
  }
}
