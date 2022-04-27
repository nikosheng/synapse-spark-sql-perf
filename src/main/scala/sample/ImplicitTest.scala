package sample

object ImplicitTest {
  implicit class MyRichInt(self: Int) {
    def myMin (num: Int): Int = {
      if (self > num) num else self
    }
  }

  def main(args: Array[String]): Unit = {
//    implicit def convert(arg: Int): MyRichInt = {
//      new MyRichInt(arg)
//    }
    println(2.myMin(6))
  }


}
