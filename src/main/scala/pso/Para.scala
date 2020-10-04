package pso

/**
  * some parameters
  */
object Para {

  val D: Map[Int, Int] = Map(12 -> 2, 180 -> 3) // 2 || 3
  val S = Map(12 -> 3, 180 -> 20) // 3 || 20
  val A = Map(12 -> 2, 180 -> 3) // 2 || 3
  //val U = 180//stage number 12 || 180
  val B: Map[Int, Array[Int]] = Map(12 -> Array(100, 50), 180 -> Array(560, 300, 140)) // (100,50) || (560,300,140)
  val m = Map(12 -> Array(5, 4), 180 -> Array(6, 5, 4)) // (5,4) || (6,5,4)
  val M = Map(12 -> Array(350, 320), 180 -> Array(3500, 1600, 500)) // (350,320) || (3500,1600,500)
  val c = Map(12 -> Array(20, 30), 180 -> Array(20, 30, 40)) // (20,30) || (20,30,40)
  val Cmax = Map(12 -> 3800, 180 -> 25000) // 3800 || 25000
  val t = Map(12 -> Array(34, 51), 180 -> Array(32, 48, 72)) //(34,51) || (32,48,72)
  val best_result_num = 1 //best results num
  val local_result_num = 1 //local best result num for one iteration

  val MAX_PENALTY = 100
  val MIN_PENALTY = 100
  val w : Double = 1 //惯性系数
  val c1 = 2.0
  val c2 = 2.0 //加速系数，同上
  val Vmax = 6.0 //粒子速度的范围
  val c0 = 0.9

  //SCA用的变量
  val a_const:Int = 5
  val r3max:Int = 10
  val M_PI = 3.14159265358979323846

}
