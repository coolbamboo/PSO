package pso

import java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import pso.Utils.getBestPop

class Run(iter_num: Int, pop_num: Int, stagenum: Int, seleAlgo: String, b_reduction: Broadcast[Array[Double]],
          b_dsak_j: Broadcast[Array[DSAK_Jup]], b_avs: Broadcast[Array[AVS]], b_sang: Broadcast[Array[SANG]],
          poplbestaccu: PopLBestAccumulator, popbestaccu: PopBestAccumulator) extends Serializable {

  def initialize(sc: SparkContext): RDD[(Int, IPop)] = {
    val task_num = sc.getConf.getInt("spark.default.parallelism", 1)
    val popsRDD: RDD[(Int, IPop)] = sc.parallelize(0 until pop_num, task_num)
      .map { i =>
        seleAlgo match {
          case "IPSO" =>
            val pop: IPop = new Pop(stagenum, b_reduction.value, i, iter_num,
              b_dsak_j.value, b_avs.value, b_sang.value,
              poplbestaccu, popbestaccu)
            (i % task_num, pop)
          case "SCA" =>
            val pop: IPop = new Pop(stagenum, b_reduction.value, i, iter_num,
              b_dsak_j.value, b_avs.value, b_sang.value,
              poplbestaccu, popbestaccu) with SCA
            (i % task_num, pop)
          case _ =>
            val pop: IPop = new Pop(stagenum, b_reduction.value, i, iter_num,
              b_dsak_j.value, b_avs.value, b_sang.value,
              poplbestaccu, popbestaccu)
            (i % task_num, pop)
        }
      } //生成RDD
    //第一次迭代，初始化
    val firstPopsRDD = popsRDD.mapValues(pop => {
      pop.initialize()
      pop.computeObj()
      pop.update_best()
      pop
    })
    firstPopsRDD.persist()
  }

  def deal_Iteration(popsRDD: RDD[(Int, IPop)], iternum: Int): RDD[(Int, IPop)] = {
    //除了第一次，不需要再次初始化
    val otherIterPopsRDD = popsRDD.mapValues(pop => {
      pop.setIter(iternum)
      pop.fly()
      pop.computeObj()
      pop.update_best()
      pop
    })
    otherIterPopsRDD.persist()
  }
}

object Run {
  def apply(sc: SparkContext, iter_num: Int, pop_num: Int, stagenum: Int, seleAlgo: String, record: Record,
            b_reduction: Broadcast[Array[Double]],
            b_dsak_j: Broadcast[Array[DSAK_Jup]], b_avs: Broadcast[Array[AVS]], b_sang: Broadcast[Array[SANG]],
            poplbestaccu: PopLBestAccumulator, popbestaccu: PopBestAccumulator): Unit = {
    val run = new Run(iter_num, pop_num, stagenum, seleAlgo, b_reduction, b_dsak_j, b_avs, b_sang, poplbestaccu, popbestaccu)
    val popRDD = run.initialize(sc)
    for (iternum <- 1 to iter_num) {
      val starttime = new Date().getTime
      val iterPopRDD = run.deal_Iteration(popRDD, iternum)
      //求最大值
      val bestLocalPopRDD = iterPopRDD.foldByKey(new Pop(stagenum, b_reduction.value, 40, iter_num,
        b_dsak_j.value, b_avs.value, b_sang.value,
        poplbestaccu, popbestaccu))((a, b) => {
        if (a.obj_F >= b.obj_F)
          a
        else
          b
      })
      //action
      val best_local_pop = bestLocalPopRDD.collect()(0)._2
      if (best_local_pop.obj_F >= getBestPop(popbestaccu.value).obj_F)
        record.gbiter = iternum
      val stoptime = new Date().getTime
      val time_interval = stoptime - starttime
      //every iter max time_interval
      if (time_interval > record.time_everyiter)
        record.time_everyiter = time_interval
    }
  }
}
