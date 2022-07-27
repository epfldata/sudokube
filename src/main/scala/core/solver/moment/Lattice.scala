package core.solver.moment

class Lattice[T](qbits: Int)(implicit val num2: Fractional[T]) {
  val N = 1 << qbits

  case class Node(key: Int) {
    var visited: Boolean = false
    var value = num2.zero
    val children = collection.mutable.ArrayBuffer[Node]()
  }

  val nodes = (0 until N).map(i => Node(i))
  (0 until N).map { i =>
    (0 until qbits).map { b =>
      val j = 1 << b
      if ((i & j) != 0) {
        val k = (i - j)
        nodes(k).children += nodes(i)
      }
    }
  }



  //(0 until N).foreach{i =>
  //  println(s"$i :: ${i.toBinaryString} ++ ${nodes(i).children.map(_.key).mkString(" ")}")
  //}
}
