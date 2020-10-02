package pso

import java.util.concurrent.ThreadLocalRandom

import pso.Para._

import scala.collection.mutable.ArrayBuffer

object Utils {

  def randval(a: Int, b:Int) :Double = {
    val r = ThreadLocalRandom.current().nextDouble(1.0)
    val value = a + (b - a) * r
    value
  }

  //四舍五入
  def randoff(r : Double) : Int = {
    val a = math.round(r)
    a.toInt
  }

  //compute J's max bound
  def deal_Jup(stagenum: Int, avss: Array[AVS], dsaks: Array[DSAK_Jup]): (Array[DSAK_Jup], Array[AVS]) = {

    val Vdsak_j = dsaks.map {
      case DSAK_Jup(num, d, s, a, kdsa, _) =>
        val gs = avss.filter(x => x.s == s).map(x => x.Gs)
        val jup = Array(
          B(stagenum)(d - 1), gs(0) / t(stagenum)(d - 1), Cmax(stagenum) / c(stagenum)(d - 1), M(stagenum)(d - 1) / m(stagenum)(d - 1)
        ).min
        if (jup < 0) throw new Exception("jup is wrong！")
        DSAK_Jup(num, d, s, a, kdsa, jup)
    }
    (Vdsak_j, avss)
  }

  def deal_reduction(stagenum: Int, dsak_j : Array[DSAK_Jup]) : Array[Double] = {
    val sum : Array[Int] = new Array(D(stagenum))
    val reduction :Array[Double] = new Array(stagenum)
    dsak_j.foreach{
      case DSAK_Jup(_,d,_,_,_,j) =>
        sum(d-1) = sum(d-1) + j
    }
    dsak_j.foreach{
      case DSAK_Jup(num,d,s,a,k,j) =>
        reduction(num-1) = 2.0 * B(stagenum)(d-1) / sum(d-1)
    }
    reduction
  }

  def addInBestPops(bestPops: ArrayBuffer[IPop], mypop: IPop, length_max: Int): Unit = {
    if (bestPops.length < length_max) {
      bestPops.append(mypop)
    }
    else {
      //if better than bestAnts,update
      var min_index = 0
      var minobj = Double.MaxValue
      for (i <- bestPops.indices) {
        if (bestPops(i).obj_F < minobj) {
          min_index = i
          minobj = bestPops(i).obj_F
        }
      }
      if (mypop.obj_F >= minobj) {
        bestPops.synchronized {
          bestPops.update(min_index, mypop)
        }
      }
    }
  }

  def getBestPop(bestpops: ArrayBuffer[IPop]): IPop = {
    bestpops.max(new Ordering[IPop] {
      def compare(a: IPop, b: IPop): Int = a.obj_F compare b.obj_F
    })
  }

  def saveToLocal(outputs : Vector[Output]): Unit ={
    import java.io.{BufferedWriter, FileOutputStream, IOException, OutputStreamWriter}
    var out : BufferedWriter = null
    try {
      out = new BufferedWriter(
        new OutputStreamWriter(
          new FileOutputStream("./results.csv", true)))
      outputs.foreach(output => {
        out.write(output.toString() + "\r\n")
      })
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally try
      out.close()
    catch {
      case e: IOException =>
        e.printStackTrace()
    }
  }

}
