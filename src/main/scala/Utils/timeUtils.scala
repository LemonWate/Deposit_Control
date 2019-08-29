package Utils

import java.text.SimpleDateFormat

/**
  * Author HanDong
  *
  * Date 2019/8/28 
  *
  * Description 
  **/
object timeUtils {
  def diffTime(start:String,end:String):Double={
    val format = new SimpleDateFormat("yyyyMMddHHmmssSSS")
    val startT: Double = format.parse(start.substring(0,17)).getTime
    val endT: Double = format.parse(end.substring(0,17)).getTime

    (endT - startT)/6000
  }


}
