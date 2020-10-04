package pso

import pso.Para._
import pso.Utils.{getBestPop, randoff, randval}

import scala.collection.mutable.ArrayBuffer

trait SCA extends IPSO {

  override def initLEN_PARTICLE(init : Int): Int ={
    init
  }

  override def decode(p: Array[Int]): Array[Int] = {
    p
  }

  override def fly(pop: IPop, poplbest : Array[ArrayBuffer[IPop]], popbest : ArrayBuffer[IPop]) : Unit = {
    val r1 : Double = a_const - 1.0 * a_const * pop.iter / pop.MAX_ITERS
    //r1 = a_const - a_const * (MAXITERS - t) / MAXITERS
    val r2 : Double = randval(0, 2 * M_PI)
    //r3 = randval(-r3max, r3max)
    val r3 : Double = 0.9
    val r4 = randval(0, 1)
    if (r4 < 0.5)
    { //sin,eq3.1
      for (i <- 1 to pop.LEN_PARTICLE)
      {
        pop.p(i-1) = randoff(pop.p(i-1) + r1 * math.sin(r2) * math.abs(r3 * getBestPop(popbest).p(i-1) - pop.p(i-1)))
        if (pop.p(i-1) < 0)
          pop.p(i-1) = 0
        if (pop.p(i-1) > pop.Jup(i-1))
          pop.p(i-1) = pop.Jup(i-1)
      }
    }
    else
    { //cos, eq3.2
      for (i <- 1 to pop.LEN_PARTICLE)
      {
        //swarm[i].p[j] = randoff(swarm[i].p[j] + r1*cos(r2)*fabs(r3*gbest_p[j]-swarm[i].p[j]));//
        pop.p(i-1) = randoff(pop.p(i-1) + r1 * math.cos(r2) * math.abs(r3 * getBestPop(poplbest(pop.id)).p(i-1) - pop.p(i-1)))
        if (pop.p(i-1) < 0)
          pop.p(i-1) = 0
        if (pop.p(i-1) > pop.Jup(i-1))
          pop.p(i-1) = pop.Jup(i-1)
      }
    }
  }
}
