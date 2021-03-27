package pso

import pso.Para._
import pso.Utils.{randoff, randval}

import scala.collection.mutable.ArrayBuffer
import scala.math.pow

/**
 *
 * @param stagenum 变量个数（阶段个数）
 * @param reduction 初始的X值
 * @param id 本粒子的编号。从0开始编号，方便一些数组的使用
 * @param MAX_ITERS 最大迭代次数
 */
class Pop(val stagenum : Int, override val reduction : Array[Double], override val id : Int, override val MAX_ITERS : Int,
          val dsak_j : Array[DSAK_Jup], val avss:Array[AVS], val sangs:Array[SANG]) extends Serializable with IPop with IPSO {

  override var iter : Int = 0 //当前迭代次数，被设置进来的
  override var LEN_PARTICLE: Int = initLEN_PARTICLE(stagenum)
  override val Jup: Array[Int] = dsak_j.map(_.Jup)
  override var Xdsa: Array[Int] = new Array(stagenum)
  //object fun
  var obj_f: Double = 0.0
  override var obj_F: Double = 0.0 //减惩罚项后
  //已经使用的资源：
  //D(defend weapon amount)
  override var b_s: Array[Double] = new Array(D(stagenum))
  //Ground
  override var g_s:Array[Double] = new Array(S(stagenum))
  //C
  override var c_s:Array[Double] = new Array(D(stagenum))
  //Manpower
  override var m_s:Array[Double] = new Array(D(stagenum))

  override val p : Array[Int] = new Array(LEN_PARTICLE)
  override val v : Array[Int] = new Array(LEN_PARTICLE)

  override def initialize(): Unit = {
    //初始化1、p
    dsak_j.foreach{
      case DSAK_Jup(num,d,s,a,k,j) =>
        p(num-1) = randoff(reduction(num-1) * randval(0,j)) //初始化位置
        v(num-1) = 0
    }
  }

  private def computeObj_fNoPenal(): Unit ={
    //解码,解码的目的是为了赋值给X
    Xdsa = decode(p)
    var f = 0.0
    avss.foreach(avs=>{
      val s = avs.s
      val vs = avs.v
      //attacker 's TT
      var attack:Double = 1.0
      sangs.filter(x=>x.s==s).foreach(sang => {
        val a = sang.a
        val nsa = sang.nsa
        val gsa = sang.gsa
        //defense:（1-kdsa）xdsa/nsa TT
        var defense:Double = 1.0
        dsak_j.filter(dsak => dsak.s==s && dsak.a==a).foreach{ dsak1 => {
          val num = dsak1.num
          val kdsa = dsak1.Kdsa
          defense = defense * pow(1.0 - kdsa, Xdsa(num - 1) / nsa.toDouble)
        }
        }
        attack = attack*pow(1.0-defense*gsa,nsa)
      })
      f = f + vs * attack
    })
    obj_f = f / avss.map(_.v).sum
  }
  //计算已经使用的资源
  private def constraintUsed(): Unit ={
    //D
    for(i <- b_s.indices){
      b_s(i) = dsak_j.filter(_.d == i+1).map(dsak => Xdsa(dsak.num-1)).sum
    }
    //C
    for(i <- c_s.indices){
      c_s(i) = dsak_j.filter(_.d == i+1).map(dsak => c(stagenum)(i) * Xdsa(dsak.num-1)).sum
    }
    //M
    for(i <- m_s.indices){
      m_s(i) = dsak_j.filter(_.d == i+1).map(dsak => m(stagenum)(i) * Xdsa(dsak.num-1)).sum
    }
    //G
    for(i <- g_s.indices){
      g_s(i) = dsak_j.filter(_.s == i+1).map(dsak => t(stagenum)(dsak.d-1) * Xdsa(dsak.num-1)).sum
    }
  }
  //先算完obj_f和使用的资源后(用到iter是当前迭代次数)
  private def computeObj_F():Unit = {
    //惩罚因子，各种资源的
    var P_B:Double = 0
    var P_G:Double = 0
    var P_C:Double = 0
    var P_M:Double = 0
    val penalty_factor = MIN_PENALTY + (MAX_PENALTY - MIN_PENALTY) * iter * 1.0 / MAX_ITERS
    for(i <- 1 to D(stagenum)){
      P_B += penalty_factor * math.max(0, 1.0 * b_s(i-1) / B(stagenum)(i-1) - 1)
      P_M += penalty_factor * math.max(0, 1.0 * m_s(i-1) / M(stagenum)(i-1) - 1)
    }
    val gs = avss.map(_.Gs)
    for(i <- 1 to S(stagenum)){
      P_G += penalty_factor * math.max(0, 1.0 * g_s(i-1) / gs(i-1) - 1)
    }
    P_C += penalty_factor * math.max(0, 1.0 * c_s.sum / Cmax(stagenum) - 1)
    obj_F = obj_f - P_B - P_G - P_C - P_M
  }

  override def computeObj(): Unit = {
    computeObj_fNoPenal()
    constraintUsed()
    computeObj_F()
  }

  override def fly(poplbest : Array[ArrayBuffer[IPop]], popbest : ArrayBuffer[IPop]) : Unit = {
    fly(pop = this, poplbest, popbest)
  }

  override def update_accu(poplbestaccu : PopLBestAccumulator, popbestaccu : PopBestAccumulator, prePops : PopPreAccumulator):Unit = {
    //可以加入(先要解码，再算obj后)
    poplbestaccu.add(this)
    popbestaccu.add(this)
    prePops.add(this)
  }

  override def update_basic(poplbest : PopLBest, popbest : PopBest, prePops : PopPre):Unit = {
    //可以加入(先要解码，再算obj后)
    poplbest.add(this)
    popbest.add(this)
    prePops.add(this)
  }

  override def setIter(nowIter: Int): Unit = {
    iter = nowIter
  }

  //override def getIter(): Int = iter
}
