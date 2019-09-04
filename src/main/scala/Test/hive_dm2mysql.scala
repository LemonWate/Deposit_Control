package Test

import java.io.{FileInputStream, InputStream}
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author HanDong
  *
  * Date 2019/9/4 
  *
  * Description 
  **/
object hive_dm2mysql {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")


    val sparkSession: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val properties = new Properties()
    properties.load(new FileInputStream("D:\\Git\\Gp_22_Deposit_Control\\src\\main\\resources\\dm2mysql.properties"))
    val dm_actlog_view: String = properties.getProperty("dm_actlog_view")
    val dm_actlog_view_region: String = properties.getProperty("dm_actlog_view_region")




    val df: DataFrame = sparkSession.sql(dm_actlog_view_region)


    df.write.mode(SaveMode.Append).jdbc(getConn._2,"dm_actlog_view_region",getConn._1)


  }
  //获取MySQL连接
  def getConn:(Properties,String)={
    val prop = new Properties()
    prop.put("user","root")
    prop.put("password","123456")
    val url = "jdbc:mysql://hdp:3306/shop?useUnicode=true&characterEncoding=utf-8&useSSL=false"
    (prop,url)
  }

}
