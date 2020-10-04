package pso

import scala.collection.mutable.ArrayBuffer

trait BPSO extends IPSO {

  override def initLEN_PARTICLE(init : Int): Int ={
    init
  }

  override def decode(p: Array[Int]): Array[Int] = {
    p
  }

  override def fly(pop: IPop, poplbest : Array[ArrayBuffer[IPop]], popbest : ArrayBuffer[IPop]) : Unit = {

  }
}
