package frontend.schema.dork
import java.io._

abstract class A extends Serializable

class A1 extends A

class A2(l: List[A]) extends A {
  def f() : List[A1] = {

    l.map{ x =>
      if(x.isInstanceOf[A2]) x.asInstanceOf[A2].f()
      else List(x.asInstanceOf[A1])
    }.flatten

    List(new A1)
  }
}


class B(l: List[A]) extends Serializable { val foo = new A2(l) }


object Tst {
  def run : B = {
    val b = new B(List(new A1))

    val oos = new ObjectOutputStream(new FileOutputStream("moo"))
    oos.writeObject(b)
    oos.close

    val ois = new ObjectInputStream(new FileInputStream("moo"))
    val b2 = ois.readObject.asInstanceOf[B]
    ois.close
    b2
  }
}


/*
frontend.schema.dork.Tst.run
*/


