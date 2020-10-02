package pso

trait IPop {

  val id: Int
  val reduction: Array[Double]
  var LEN_PARTICLE: Int
  var Xdsa: Array[Int]
  val Jup: Array[Int]
  var obj_F: Double
  val p: Array[Int]
  val v: Array[Int]

  var b_s: Array[Double]
  var g_s: Array[Double]
  var c_s: Array[Double]
  var m_s: Array[Double]

  def initLEN_PARTICLE(init: Int): Int

  def initialize()

  def computeObj()

  def decode(p: Array[Int]): Array[Int]

  def fly()

  def update_best()

  def setIter(nowIter : Int)

  //def getIter() : Int
}
