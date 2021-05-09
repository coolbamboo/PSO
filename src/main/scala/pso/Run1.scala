package pso

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import pso.Utils.getBestPop

import java.util.Date
import scala.collection.mutable.ArrayBuffer

class Run1(iter_num: Int, pop_num: Int, stagenum: Int, seleAlgo: String, b_reduction: Broadcast[Array[Double]],
           b_dsak_j: Broadcast[Array[DSAK_Jup]], b_avs: Broadcast[Array[AVS]], b_sang: Broadcast[Array[SANG]]) extends Serializable {

  def deal_Iteration(sc: SparkContext, iternum: Int, prePop : Array[ArrayBuffer[IPop]]
                    , poplbestaccuV : Array[ArrayBuffer[IPop]], popbestaccuV : ArrayBuffer[IPop]): RDD[IPop] = {
    val task_num = sc.getConf.getInt("spark.default.parallelism", 1)
    val popsRDD: RDD[IPop] = sc.parallelize(0 until pop_num, task_num)
      .map { i =>
        seleAlgo match {
          case "IPSO" =>
            val pop: IPop = new Pop(stagenum, b_reduction.value, i, iter_num,
              b_dsak_j.value, b_avs.value, b_sang.value)
            pop
          case "SCA" =>
            val pop: IPop = new Pop(stagenum, b_reduction.value, i, iter_num,
              b_dsak_j.value, b_avs.value, b_sang.value) with SCA
            pop
          case "BPSO" =>
            val pop: IPop = new Pop(stagenum, b_reduction.value, i, iter_num,
              b_dsak_j.value, b_avs.value, b_sang.value) with BPSO
            pop
          case _ =>
            val pop: IPop = new Pop(stagenum, b_reduction.value, i, iter_num,
              b_dsak_j.value, b_avs.value, b_sang.value)
            pop
        }
      } //生成RDD
    //第一次迭代，初始化
    if(iternum == 0) {
      val firstPopsRDD = popsRDD.mapPartitions(pops => pops.map { pop =>
        //val firstPopsRDD = popsRDD.map(pop => {
        pop.initialize()
        pop.computeObj()
        pop
      }
      )
      //firstPopsRDD.persist()
      firstPopsRDD
    }else{
      //除了第一次，不需要再次初始化
      //val otherIterPopsRDD = popsRDD.map(pop => prePops.value(pop.id).head).map{ pop =>
      val otherIterPopsRDD = popsRDD.mapPartitions(pops => pops.map { pop =>
        val newpop = prePop(pop.id).head
        newpop.setIter(iternum)
        newpop.fly(poplbestaccuV, popbestaccuV)
        newpop.computeObj()
        newpop
      })
      //otherIterPopsRDD.persist()
      otherIterPopsRDD
    }
  }
}

object Run1 {
  def apply(sc: SparkContext, iter_num: Int, pop_num: Int, stagenum: Int, seleAlgo: String, record: Record,
            b_reduction: Broadcast[Array[Double]],
            b_dsak_j: Broadcast[Array[DSAK_Jup]], b_avs: Broadcast[Array[AVS]], b_sang: Broadcast[Array[SANG]],
            poplbestaccu: PopLBestAccumulator, popbestaccu: PopBestAccumulator, prePops : PopPreAccumulator): Unit = {
    val run = new Run1(iter_num, pop_num, stagenum, seleAlgo, b_reduction, b_dsak_j, b_avs, b_sang)
    //0是初始化要做的工作
    for (iternum <- 0 to iter_num) {
      val starttime = new Date().getTime
      val iterPopRDD = run.deal_Iteration(sc, iternum, prePops.value, poplbestaccu.value, popbestaccu.value)
      //更新累加器
      iterPopRDD.foreach(pop => pop.update_accu(poplbestaccu, popbestaccu, prePops))
      //求最大值
      //val bestLocalPopRDD = iterPopRDD.sortBy(pop => pop.obj_F, ascending = false) //用这个sortBy transform有bug
      val bestLocalPop = iterPopRDD.max()(new Ordering[IPop](){
        override def compare(x: IPop, y: IPop): Int = {
          if(x.obj_F < y.obj_F) -1
          else if(x.obj_F > y.obj_F) 1
          else 0
        }
      }) //获取数据后比较大小
      //action
      //val best_local_pop = bestLocalPopRDD.take(1)(0)
      //if (best_local_pop.obj_F >= getBestPop(popbestaccu.value).obj_F)
      if(bestLocalPop.obj_F >= getBestPop(popbestaccu.value).obj_F)
        record.gbiter = iternum
      val stoptime = new Date().getTime
      val time_interval = stoptime - starttime
      //every iter max time_interval
      if (time_interval > record.time_everyiter)
        record.time_everyiter = time_interval
    }
  }
}


