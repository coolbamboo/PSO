package pso

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import pso.Para.{best_result_num, local_result_num}
import pso.Utils.{deal_Jup, deal_reduction}

import java.util.Date

object mainbasic {

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

    val conf = new SparkConf().setAppName("WTA_PSO")
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

    val sc = new SparkContext(conf)
    //read data
    val (rawAVS, rawDSAK, rawSANG) = new ReadData(dataPath.trim).apply(sc, stagenum)
    val avses = rawAVS.collect()
    val dsaks = rawDSAK.collect()
    val sang = rawSANG.collect()
    //ready to record
    val record = new Record()
    val begintime = new Date().getTime
    val runRDD: RDD[Vector[Output]] = sc.parallelize(1 to runmax_num, task_num).map { i =>
      println(s"第${i}次运行：")
      record.now_run = i
      //begin
      val starttime = new Date().getTime
      //初始化：1、求j的上限
      val (dsak_j, avs) = deal_Jup(stagenum, avses, dsaks)
      //初始化：2、求解的最初值(根据导数)
      val reduction = deal_reduction(stagenum, dsak_j)
      val globalBestPops = new PopBest(best_result_num)
      val localBestPops = new PopLBest(pop_num, local_result_num)
      val prePops = new PopPre(pop_num)
      //准备：调用运行机制
      RunForBasic(iter_num, pop_num, stagenum, seleAlgo, record,
        reduction, dsak_j, avs, sang,
        localBestPops, globalBestPops, prePops)
      //end
      val stoptime = new Date().getTime
      record.time_runonce = stoptime - starttime
      //output
      val outputs: Vector[Output] = Output(stagenum, iter_num, pop_num, task_num,
        dataPath, runStyle, seleAlgo, record, globalBestPops.value).sortWith(_.pop.obj_F > _.pop.obj_F)
      outputs
    }
    val outputs = runRDD.collect()
    val endtime = new Date().getTime
    println(s"run time:${endtime-begintime}毫秒")
    outputs.foreach(outputs => {
      runStyle.trim match {
        case "local" =>
          Utils.saveToLocal(outputs)
        case _ =>
          //val hdfs_path = "hdfs://192.168.30.50:9000/WTA/data/output/"
          //sc.parallelize(outputs, 1).saveAsTextFile(hdfs_path)
          val outputfile = "/home/spark/Downloads/results.csv"
          Utils.saveToLocal(outputs, outputfile)
      }
    })

    sc.stop()
  }
}
