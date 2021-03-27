package pso

import scala.collection.mutable.ArrayBuffer

class PopBest(private val lenth_max: Int) {

  private var bestPops: ArrayBuffer[IPop] = ArrayBuffer[IPop]()
  private val bound = lenth_max

  def isZero: Boolean = bestPops.isEmpty

  def copy(): PopBest = {
    val newAcc = new PopBest(bound)
    bestPops.synchronized {
      newAcc.bestPops.appendAll(bestPops)
    }
    newAcc
  }

  def reset(): Unit = {
    bestPops.clear()
  }

  def add(v: IPop): Unit = {
    Utils.addInBestPops(bestPops, v, bound)
  }

  def merge(other: PopBest): Unit = {
    other match {
      case o: PopBest =>
        bestPops.appendAll(o.value)
        if (bestPops.length > bound)
        //sort and slice
          bestPops = bestPops.sortWith((a, b) => a.obj_F > b.obj_F).take(bound)
    }
  }

  def value: ArrayBuffer[IPop] = bestPops
}
