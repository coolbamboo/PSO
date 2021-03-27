package pso

import scala.collection.mutable.ArrayBuffer

//记住上一次迭代时粒子的状态
class PopPre(val popnum : Int) {

  private val prePops : Array[ArrayBuffer[IPop]] = new Array(popnum)
  for(i <- 0 until popnum){
    prePops(i) = ArrayBuffer[IPop]()
  }
  private val bound = 1 //只存前一次迭代的一个值

  def isZero: Boolean = {
    var isZero = true
    prePops.foreach(x => {
      if(x.nonEmpty) {
        isZero = false
        return isZero
      }
    })
    isZero
  }

  def copy(): PopPre = {
    val newAcc = new PopPre(popnum)
    for(i <- 1 to popnum)
      prePops(i-1).synchronized {
        newAcc.prePops(i-1).appendAll(prePops(i-1))
      }
    newAcc
  }

  def reset(): Unit = {
    for(i <- 0 until popnum){
      prePops(i) = ArrayBuffer[IPop]()
    }
  }

  def add(v: IPop): Unit = {
    val id = v.id
    if (prePops(id).length < bound) {
      prePops(id).append(v)
    }
    else {
      prePops(id).update(0, v)
    }
  }

  def merge(other: PopPre): Unit = {
    other match {
      case o: PopPre =>
        for(i <- 1 to popnum) {
          prePops(i-1).appendAll(o.value(i-1))
          if (prePops(i-1).length > bound)
              //sort and slice
              prePops(i-1) = prePops(i-1).sortWith((a, b) => a.iter > b.iter || a.obj_F >= b.obj_F).take(bound)
        }
    }
  }

  def value: Array[ArrayBuffer[IPop]] = prePops
}
