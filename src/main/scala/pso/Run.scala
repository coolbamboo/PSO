package pso

import java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object Run {
  def apply(sc: SparkContext, iter_num: Int, pop_num: Int, stagenum:Int,seleAlgo: String, record: Record,
            b_reduction: Broadcast[Array[Double]],
            b_dsak_j: Broadcast[Array[DSAK_Jup]], b_avs: Broadcast[Array[AVS]], b_sang: Broadcast[Array[SANG]],
           poplbestaccu : PopLBestAccumulator, popbestaccu : PopBestAccumulator): Unit = {
    val task_num = sc.getConf.getInt("spark.default.parallelism", 1)
    for (iternum <- 1 to iter_num) {
      val starttime = new Date().getTime
      val popsRDD: RDD[(Int, IPop)] = sc.parallelize(0 until pop_num, task_num)
        .map { i =>
          seleAlgo match {
            case "IPSO" =>
              val pop: IPop = new Pop(stagenum, b_reduction.value, i, iternum, iter_num,
                b_dsak_j.value, b_avs.value, b_sang.value,
                poplbestaccu, popbestaccu)
              (i % task_num, pop)
            case "SCA" =>
              val pop: IPop = new Pop(stagenum, b_reduction.value, i, iternum, iter_num,
                b_dsak_j.value, b_avs.value, b_sang.value,
                poplbestaccu, popbestaccu) with SCA
              (i % task_num, pop)
            case _ =>
              val pop: IPop = new Pop(stagenum, b_reduction.value, i, iternum, iter_num,
                b_dsak_j.value, b_avs.value, b_sang.value,
                poplbestaccu, popbestaccu)
              (i % task_num, pop)
          }
        }//生成RDD
      if(iternum == 1) {
        //第一次迭代，初始化
        popsRDD.mapValues(pop => {
          pop.initialize()
          pop.computeObj()
          pop.update_best()
          pop
        })
      }else{
        popsRDD.mapValues(pop => {
          pop.fly()
          pop.computeObj()
          pop.update_best()
          pop
        })
      }
      //action
      popsRDD.take(1)
      val stoptime = new Date().getTime
      val time_interval = stoptime - starttime
      //every iter max time_interval
      if(time_interval > record.time_everyiter)
        record.time_everyiter = time_interval
    }
  }
}
