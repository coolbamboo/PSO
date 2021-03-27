package pso

import scala.collection.mutable.ArrayBuffer

class PopLBest(val popnum : Int, private val lenth_max : Int) {

  private val bestPops : Array[ArrayBuffer[IPop]] = new Array(popnum)
  for(i <- 0 until popnum){
    bestPops(i) = ArrayBuffer[IPop]()
  }
  private val bound = lenth_max

  def isZero: Boolean = {
    var isZero = true
    bestPops.foreach(x => {
      if(x.nonEmpty) {
        isZero = false
        return isZero
      }
    })
    isZero
  }

  def copy(): PopLBest = {
    val newAcc = new PopLBest(popnum, bound)
    for(i <- 1 to popnum)
      bestPops(i-1).synchronized {
        newAcc.bestPops(i-1).appendAll(bestPops(i-1))
      }
    newAcc
  }

  def reset(): Unit = {
    for(i <- 0 until popnum){
      bestPops(i) = ArrayBuffer[IPop]()
    }
  }

  def add(v: IPop): Unit = {
    val id = v.id
    Utils.addInBestPops(bestPops(id), v, bound)
  }

  def merge(other: PopLBest): Unit = {
    other match {
      case o: PopLBest =>
        for(i <- 1 to popnum) {
          bestPops(i-1).appendAll(o.value(i-1))
          if (bestPops(i-1).length > bound)
              //sort and slice
              bestPops(i-1) = bestPops(i-1).sortWith((a, b) => a.obj_F > b.obj_F).take(bound)
        }
    }
  }

  def value: Array[ArrayBuffer[IPop]] = bestPops
}
