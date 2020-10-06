package pso

import pso.Para.{Vmax, c0, c1, c2, w}
import pso.Utils.{getBestPop, randoff, randval}

import scala.collection.mutable.ArrayBuffer

trait BPSO extends IPSO {

  override def initLEN_PARTICLE(init : Int): Int ={
    init
  }

  override def decode(p: Array[Int]): Array[Int] = {
    p
  }

  override def fly(pop: IPop, poplbest : Array[ArrayBuffer[IPop]], popbest : ArrayBuffer[IPop]) : Unit = {
    val r0: Double = randval(0, 1)
    if (r0 < c0) {
      val r1: Double = randval(0, 1)
      val r2: Double = randval(0, 1)
      for (i <- 1 to pop.LEN_PARTICLE) {
        var study_v = w * pop.v(i - 1) + c1 * r1 * (getBestPop(poplbest(pop.id)).p(i - 1) - pop.p(i - 1)) +
          c2 * r2 * (getBestPop(popbest).p(i - 1) - pop.p(i - 1))
        if (study_v > Vmax) study_v = Vmax
        if (study_v < -Vmax) study_v = -Vmax
        pop.v(i - 1) = randoff(study_v)
        val binaryIntArray = decimalToBinary(pop.p(i-1))
        for(j <- binaryIntArray.indices) {
          val sig = 1.0 / (1 + math.exp(-study_v))
          val p = randval(0, 1)
          if(p < sig) binaryIntArray(j) = 1
          else binaryIntArray(j) = 0
        }
        pop.p(i-1) = binaryintarrayToInt(binaryIntArray)
        if (pop.p(i - 1) < 0) {
          pop.p(i - 1) = 0
        }
        if (pop.p(i - 1) > pop.Jup(i - 1)) {
          pop.p(i - 1) = pop.Jup(i - 1)
        }
      }
    } else {
      for (i <- 1 to pop.LEN_PARTICLE) {
        pop.p(i - 1) = randoff(pop.reduction(i - 1) * randval(0, pop.Jup(i - 1))) //随机产生初始位置，初始化
        pop.v(i - 1) = 0; // 初始化速度也为0
      }
    }
  }

  private def decimalToBinary(input : Int) : Array[Int] = {
    val input_s = input.toBinaryString
    val output : Array[Int] = input_s.map(x => x.toInt).toArray
    output
  }

  private def binaryintarrayToInt(input: Array[Int]) : Int = {
    var sum:Double = 0
    val length = input.length
    for (i <- input.indices){
      //第i位 的数字为：
      val dt = input(i)
      sum += math.pow(2,length- i -1) * dt
    }
    sum.toInt
  }
}
