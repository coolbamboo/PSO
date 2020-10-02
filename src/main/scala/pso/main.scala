package pso

import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}
import pso.Para.{best_result_num, local_result_num}
import pso.Utils.{deal_Jup, deal_reduction}

object main {

  /**
   * imported parameter:
   * * args0: stage num(variable num)
   * * args1: iteration num
   * * args2: pop num(every iteration)
   * * args3: task_num(whether partition and how many pars)
   * * args4: dataPath,data file is sourcing from local(fileurl) or "hdfs"
   * * args5: "local" running , or other running style
   * args6: select algorithm,IPSO,SCA...
   * args7: max run num
   * e.x. 12 1000 40 4 D:\myProjects\MMAS2\datafiles\ local IPSO 10
   */
  def main(args: Array[String]): Unit = {

    val stagenum: Int = args(0).toInt
    val iter_num: Int = args(1).toInt
    val pop_num: Int = args(2).toInt
    val task_num = args(3).toInt

    val dataPath = args(4)
    val runStyle = args(5)
    val seleAlgo = args(6)
    val runmax_num = args(7).toInt


    val conf = new SparkConf().setAppName("WTA")
    if (task_num > 0)
      conf.set("spark.default.parallelism", task_num.toString)
    else
      conf.set("spark.default.parallelism", "1")

    if ("local".equals(runStyle.trim)) {
      if (task_num > 0) {
        conf.setMaster(s"local[$task_num]") //local run
      } else {
        conf.setMaster("local[1]") //local run
      }
    }
    for (run <- 1 to runmax_num) {
      println(s"第${run}次运行：")
      val sc = new SparkContext(conf)
      //ready to record
      val record = new Record()
      record.now_run = run
      //read data
      val (rawAVS, rawDSAK, rawSANG) = new ReadData(dataPath.trim).apply(sc, stagenum)
      val avses = rawAVS.collect()
      val dsaks = rawDSAK.collect()
      val sang = rawSANG.collect()

      //begin
      val starttime = new Date().getTime
      //初始化：1、求j的上限
      val (dsak_j, avs) = deal_Jup(stagenum, avses, dsaks)
      //初始化：2、求解的最初值(根据导数)
      val reduction = deal_reduction(stagenum, dsak_j)

      //将四个数据集广播
      val b_dsak_j = sc.broadcast(dsak_j)
      val b_avs = sc.broadcast(avs)
      val b_sang = sc.broadcast(sang)
      val b_reduction = sc.broadcast(reduction)
      //两个累加器变量
      //init an accumulator
      val globalBestPops = new PopBestAccumulator(best_result_num)
      sc.register(globalBestPops, "globalBestPops")
      val localBestPops = new PopLBestAccumulator(pop_num, local_result_num)
      sc.register(localBestPops, "localBestPops")
      //准备：调用运行机制
      Run(sc, iter_num, pop_num, stagenum, seleAlgo, record: Record,
        b_reduction, b_dsak_j, b_avs, b_sang,
        localBestPops, globalBestPops)
      //end
      val stoptime = new Date().getTime
      record.time_runonce = stoptime - starttime
      //output
      var outputs: Vector[Output] = null
      outputs = Output(stagenum, iter_num, pop_num, task_num,
        dataPath, runStyle, seleAlgo, record, globalBestPops.value).sortWith(_.pop.obj_F > _.pop.obj_F)

      runStyle.trim match {
        case "local" =>
          Utils.saveToLocal(outputs)
        case _ =>
          val hdfs_path = "hdfs://192.168.120.133:8020/WTA/data/output/"
          sc.parallelize(outputs, 1).saveAsTextFile(hdfs_path)
      }

      sc.stop()
    }
  }
}
