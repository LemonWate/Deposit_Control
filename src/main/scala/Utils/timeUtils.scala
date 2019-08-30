package Utils

import java.text.SimpleDateFormat
import java.util.Date

/**
  * Author HanDong
  *
  * Date 2019/8/28 
  *
  * Description 
  **/
object timeUtils {
  def formatTime(time:String):String={
    val fmat = new SimpleDateFormat("yyyy-MM-dd")
    val startT: Date = fmat.parse(time)
    val str: String = fmat.format(startT)
    println(str)
    str

  }
}
