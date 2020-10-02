package pso


class Record extends Serializable{

  var time_everyiter : Double = 0.0 //every iteration time, max
  var time_runonce : Double = 0.0 // the ’one run‘ program run time

  var now_run : Int= 0 //现在是运行第几次
}
