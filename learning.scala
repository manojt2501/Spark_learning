

object learning extends App {
//  using yield to print unit as vector
  val b = for (i <- 1 to 10) yield{
    i*i
  }
  println(b)
//  if condition in yield
   val c = for (i <- 1 to 10) yield{
     if (i%2==0)
    i*i
  }
  println(c)
//  if guard
  val d = for (i <- 1 to 10; if (i%2==0)) yield{
     
    i*i
  }
  println(d)
}