package pso

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable.ArrayBuffer

class PopBestAccumulator(private val lenth_max: Int = 10) extends AccumulatorV2[IPop, ArrayBuffer[IPop]] {

  private var bestPops: ArrayBuffer[IPop] = ArrayBuffer[IPop]()
  private val bound = lenth_max

  override def isZero: Boolean = bestPops.isEmpty

  override def copy(): AccumulatorV2[IPop, ArrayBuffer[IPop]] = {
    val newAcc = new PopBestAccumulator(bound)
    bestPops.synchronized {
      newAcc.bestPops.appendAll(bestPops)
    }
    newAcc
  }

  override def reset(): Unit = {
    bestPops.clear()
  }

  override def add(v: IPop): Unit = {
    Utils.addInBestPops(bestPops, v, bound)
  }

  override def merge(other: AccumulatorV2[IPop, ArrayBuffer[IPop]]): Unit = {
    other match {
      case o: PopBestAccumulator =>
        bestPops.appendAll(o.value)
        if (bestPops.length > bound)
        //sort and slice
          bestPops = bestPops.sortWith((a, b) => a.obj_F > b.obj_F).take(bound)
    }
  }

  override def value: ArrayBuffer[IPop] = bestPops
}
