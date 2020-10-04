package pso

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable.ArrayBuffer

//记住上一次迭代时粒子的状态
class PopPreAccumulator(val popnum : Int) extends AccumulatorV2[IPop, Array[ArrayBuffer[IPop]]] {

  private val prePops : Array[ArrayBuffer[IPop]] = new Array(popnum)
  for(i <- 0 until popnum){
    prePops(i) = ArrayBuffer[IPop]()
  }
  private val bound = 1 //只存前一次迭代的一个值

  override def isZero: Boolean = {
    var isZero = true
    prePops.foreach(x => {
      if(x.nonEmpty) {
        isZero = false
        return isZero
      }
    })
    isZero
  }

  override def copy(): AccumulatorV2[IPop, Array[ArrayBuffer[IPop]]] = {
    val newAcc = new PopPreAccumulator(popnum)
    for(i <- 1 to popnum)
      prePops(i-1).synchronized {
        newAcc.prePops(i-1).appendAll(prePops(i-1))
      }
    newAcc
  }

  override def reset(): Unit = {
    for(i <- 0 until popnum){
      prePops(i) = ArrayBuffer[IPop]()
    }
  }

  override def add(v: IPop): Unit = {
    val id = v.id
    if (prePops(id).length < bound) {
      prePops(id).append(v)
    }
    else {
      prePops(id).update(0, v)
    }
  }

  override def merge(other: AccumulatorV2[IPop, Array[ArrayBuffer[IPop]]]): Unit = {
    other match {
      case o: PopPreAccumulator =>
        for(i <- 1 to popnum) {
          prePops(i-1).appendAll(o.value(i-1))
          if (prePops(i-1).length > bound)
              //sort and slice
              prePops(i-1) = prePops(i-1).sortWith((a, b) => a.iter > b.iter || a.obj_F >= b.obj_F).take(bound)
        }
    }
  }

  override def value: Array[ArrayBuffer[IPop]] = prePops
}
