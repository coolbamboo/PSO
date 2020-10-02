package pso


class Record extends Serializable{

  var time_everyiter : Double = 0.0 //every iteration time, max
  var time_runonce : Double = 0.0 // the ’one run‘ program run time

  var gbiter : Int= 0 //出现最好结果的那次迭代
  var now_run : Int = 0 //当前是第几次运行
}
