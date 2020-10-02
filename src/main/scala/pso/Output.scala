package pso

import scala.collection.mutable.ArrayBuffer

/**
 * results:
  * stagenum
  * iter_num
  * ant_num
  * task_num
  * localpath,data file is sourcing from local(fileurl) or "hdfs"
  * "local" running , or other running style
  * basic run MMAS, or improved(distributed) run MMAS
  * result:"local" or save on "hdfs"
  * record
  * ant(func,...)
  */
class Output(stagenum: Int, iter_num: Int, pop_num: Int, task_num: Int,
             dataPath: String, runStyle: String, algoSele: String,
             record: Record, val pop: IPop) extends Serializable {

  override def toString: String = {
    val sb = new StringBuilder("")
    sb.append(s"当前是第${record.now_run}次运行,")
    sb.append(stagenum).append(",")
    sb.append(iter_num).append(",")
    sb.append(pop_num).append(",")
    sb.append(task_num).append(",")
    sb.append(dataPath).append(",")
    sb.append(runStyle).append(",")
    sb.append(algoSele).append(",")

    sb.append(record.time_runonce).append(",")
    sb.append(record.time_everyiter).append(",")
    sb.append(s"第${record.gbiter}次迭代获取最优解,")
    sb.append(pop.obj_F).append(",")
    sb.append(pop.b_s.toSeq.toString().replace(",", "-")).append(":X,")
    sb.append(pop.g_s.toSeq.toString().replace(",", "-")).append(":ground,")
    sb.append(pop.c_s.sum).append(":costs,")
    sb.append(pop.m_s.toSeq.toString().replace(",", "-")).append(":manpower,")
    for (i <- pop.Xdsa.indices) {
      if (pop.Xdsa(i) > 0)
        sb.append("(").append(i + 1).append("_").append(pop.Xdsa(i)).append(")")
    }
    //sb.append("\r\n")
    sb.toString()
  }

}

object Output {

  def apply(stagenum: Int, iter_num: Int, pop_num: Int, task_num: Int,
            dataPath: String, runStyle: String, algoSele: String,
            record: Record,
            bestPops: ArrayBuffer[IPop]): Vector[Output] = {
    bestPops.map(x => new Output(stagenum, iter_num, pop_num, task_num,
      dataPath, runStyle, algoSele, record, x)).toVector
  }
}
