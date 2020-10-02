package pso

import pso.Para.{c1, c2, c0}
import pso.Utils.{getBestPop, randoff, randval}

import scala.collection.mutable.ArrayBuffer

trait IPSO {

  def initLEN_PARTICLE(init : Int): Int ={
    init
  }

  def decode(p: Array[Int]): Array[Int] = {
    p
  }

  def fly(pop: IPop, poplbest : Array[ArrayBuffer[IPop]], popbest : ArrayBuffer[IPop]) : Unit = {
    val r0: Double = randval(0, 1)
    if (r0 <= c0) {
      val r1: Double = randval(0, 1)
      val r2: Double = randval(0, 1)
      for (i <- 1 to pop.LEN_PARTICLE) {
        val study_v = c1 * r1 * (getBestPop(poplbest(pop.id)).p(i - 1) - pop.p(i - 1)) +
          c2 * r2 * (getBestPop(popbest).p(i - 1) - pop.p(i - 1))
        pop.v(i - 1) = pop.v(i - 1) + randoff(study_v)
        pop.p(i - 1) = pop.p(i - 1) + pop.v(i - 1)
        if (pop.p(i - 1) < 0) {
          pop.p(i - 1) = 0
          pop.v(i - 1) = 0
        }

        if (pop.p(i - 1) > pop.Jup(i - 1)) {
          pop.p(i - 1) = pop.Jup(i - 1)
          pop.v(i - 1) = 0
        }
      }
    } else {
      for (i <- 1 to pop.LEN_PARTICLE) {
        pop.p(i - 1) = randoff(pop.reduction(i - 1) * randval(0, pop.Jup(i - 1))) //随机产生初始位置，初始化
        pop.v(i - 1) = 0; // 初始化速度也为0
      }
    }
  }
}
