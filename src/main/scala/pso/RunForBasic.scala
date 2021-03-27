package pso

import pso.Utils.getBestPop

import java.util.Date

class RunForBasic(iter_num: Int, pop_num: Int, stagenum: Int, seleAlgo: String, reduction: Array[Double],
                  dsak_j: Array[DSAK_Jup], avs: Array[AVS], sang: Array[SANG],
                  poplbest: PopLBest, popbest: PopBest, prePops : PopPre) extends Serializable {
  //初始化，生成粒子群，返回生成的粒子群
  def init(): IndexedSeq[IPop] = {
    val genepops: IndexedSeq[IPop] = (0 until pop_num)
      .map { i =>
        seleAlgo match {
          case "IPSO" =>
            val pop: IPop = new Pop(stagenum, reduction, i, iter_num,
              dsak_j, avs, sang)
            pop
          case "SCA" =>
            val pop: IPop = new Pop(stagenum, reduction, i, iter_num,
              dsak_j, avs, sang) with SCA
            pop
          case "BPSO" =>
            val pop: IPop = new Pop(stagenum, reduction, i, iter_num,
              dsak_j, avs, sang) with BPSO
            pop
          case _ =>
            val pop: IPop = new Pop(stagenum, reduction, i, iter_num,
              dsak_j, avs, sang)
            pop
        }
      }
    //第一次迭代，初始化
    val initPops = genepops.map{ pop =>
        //val firstPopsRDD = popsRDD.map(pop => {
        pop.initialize()
        pop.computeObj()
        pop.update_basic(poplbest, popbest, prePops)
        pop
    }
    initPops
  }

  def deal_Iteration(pops : IndexedSeq[IPop], iternum: Int): IndexedSeq[IPop] = {
    val flyedPops = pops.map{ pop =>
      val newpop = prePops.value(pop.id).head//从前一次迭代取值
      newpop.setIter(iternum)
      newpop.fly(poplbest.value, popbest.value)
      newpop.computeObj()
      newpop.update_basic(poplbest, popbest, prePops)
      newpop
    }
    flyedPops
  }
}

object RunForBasic {
  def apply(iter_num: Int, pop_num: Int, stagenum: Int, seleAlgo: String, record: Record,
            reduction: Array[Double],
            dsak_j: Array[DSAK_Jup], avs: Array[AVS], sang: Array[SANG],
            poplbest: PopLBest, popbest: PopBest, prePops : PopPre): Unit = {
    val run = new RunForBasic(iter_num, pop_num, stagenum, seleAlgo, reduction, dsak_j, avs, sang, poplbest, popbest,
      prePops)
    val popsInit = run.init()//初始化
    for (iternum <- 1 to iter_num) {
      val starttime = new Date().getTime
      val iterPop = run.deal_Iteration(popsInit, iternum)
      val bestLocalPop = iterPop.max(new Ordering[IPop](){
        override def compare(x: IPop, y: IPop): Int = {
          if(x.obj_F < y.obj_F) -1
          else if(x.obj_F > y.obj_F) 1
          else 0
        }
      })
      if(bestLocalPop.obj_F >= getBestPop(popbest.value).obj_F)
        record.gbiter = iternum
      val stoptime = new Date().getTime
      val time_interval = stoptime - starttime
      //every iter max time_interval
      if (time_interval > record.time_everyiter)
        record.time_everyiter = time_interval
    }
  }
}


