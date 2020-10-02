package pso

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable.ArrayBuffer

class PopLBestAccumulator(val popnum : Int, private val lenth_max : Int) extends AccumulatorV2[IPop, Array[ArrayBuffer[IPop]]] {

  private val bestPops : Array[ArrayBuffer[IPop]] = new Array(popnum)
  for(i <- 0 until popnum){
    bestPops(i) = ArrayBuffer[IPop]()
  }
  private val bound = lenth_max

  override def isZero: Boolean = {
    var isZero = true
    bestPops.foreach(x => {
      if(x.nonEmpty) {
        isZero = false
        return isZero
      }
    })
    isZero
  }

  override def copy(): AccumulatorV2[IPop, Array[ArrayBuffer[IPop]]] = {
    val newAcc = new PopLBestAccumulator(popnum, bound)
    for(i <- 1 to popnum)
      bestPops(i-1).synchronized {
        newAcc.bestPops(i-1).appendAll(bestPops(i-1))
      }
    newAcc
  }

  override def reset(): Unit = {
    for(i <- 0 until popnum){
      bestPops(i) = ArrayBuffer[IPop]()
    }
  }

  override def add(v: IPop): Unit = {
    val id = v.id
    Utils.addInBestPops(bestPops(id), v, bound)
  }

  override def merge(other: AccumulatorV2[IPop, Array[ArrayBuffer[IPop]]]): Unit = {
    other match {
      case o: PopLBestAccumulator =>
        for(i <- 1 to popnum) {
          bestPops(i-1).appendAll(o.value(i-1))
          if (bestPops(i-1).length > bound)
              //sort and slice
              bestPops(i-1) = bestPops(i-1).sortWith((a, b) => a.obj_F > b.obj_F).take(bound)
        }
    }
  }

  override def value: Array[ArrayBuffer[IPop]] = bestPops
}
