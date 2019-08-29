package Work

import java.lang

import Utils.{JedisConnectionPool, OffSetUtils, timeUtils}
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, rdd}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
  * Author HanDong
  *
  * Date 2019/8/28 
  *
  * Description 
  **/
object Survey_worke_1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      // 设置每秒每个分区拉取kafka数据的速率（即：记录的条数）
      .set("spark.streaming.kafka.maxRatePerPartition", "100")
      //设置序列化方式
      .set("spark.serlizer", "org.apache.spark.serializer.KryoSerializer")

    //创建streaming的上下文对象
    val ssc = new StreamingContext(conf, Seconds(3))

    //广播变量
    val data: RDD[String] = ssc.sparkContext.textFile("D:\\Desktop\\BigData22\\22Spark项目\\项目day07\\充值平台实时统计分析\\city.txt")
    val map: Map[String, String] = data.map(x => (x.split(" ")(0), (x.split(" ")(1)))).collect.toMap
    val bro_citydic: Broadcast[Map[String, String]] = ssc.sparkContext.broadcast(map)

    //    println(bro_citydic.value.getOrElse("200",null))


    //设置组名、topic名
    val groupId = "group01"
    val topic = "hd22pro"

    // 指定kafka的broker地址（SparkStreaming程序消费过程中，需要和Kafka的分区对应）
    val brokerList = "192.168.189.233:9092"

    // 编写Kafka的配置参数
    val kafkas = Map[String, Object](
      "bootstrap.servers" -> brokerList,
      // kafka的Key和values解码方式
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      // 从头消费
      "auto.offset.reset" -> "earliest",
      // 不需要程序自动提交Offset
      "enable.auto.commit" -> (false: lang.Boolean)
    )
    // 创建topic集合，可能会消费多个Topic
    val topics = Set(topic)

    val dStream: InputDStream[ConsumerRecord[String, String]] = OffSetUtils.getInputDStream(ssc, topics, groupId, kafkas)
    //    var list = List[String]()

    dStream.foreachRDD(rdd => {

      val allData: RDD[JSONObject] = rdd.map(_.value()).map(JSON.parseObject(_))

      val resRDD: RDD[(String, List[Double], (String, String), String,String)] = allData.filter(_.getString("serviceName").equals("reChargeNotifyReq")).map(x => {
        val result = x.getString("bussinessRst")
        val suc_money = if (result.equals("0000")) {
          x.getString("chargefee").toDouble
        } else {
          0.0
        }
        val suc_count = if (suc_money != 0.0) 1 else 0
        val pro_id: String = x.getString("provinceCode")
        val startTime = x.getString("requestId")
        val endtime = x.getString("receiveNotifyTime")
        val city = bro_citydic.value.getOrElse(pro_id, "无")
        //        val costtime = timeUtils.diffTime(startTime, endtime)


        (startTime.substring(0, 8), List(1, suc_money, suc_count), (startTime.substring(0, 10), city), city,startTime.substring(0,10))

      })

      //
      //      resultShow.workSurvey1_1(resRDD)
      //      resultShow.workSurvey1_2(resRDD)
//      resultShow.workSurvey2(resRDD)
//      resultShow.workSurvey3(resRDD)
      resultShow.workSurvey4(resRDD.map(x=>(x._5,x._2)).reduceByKey((list1,list2)=>{list1.zip(list2).map(t=>t._1+t._2)}))




      //将偏移量进行更新

      OffSetUtils.saveOfferToRedis(rdd, groupId)

    })


    ssc.start()
    ssc.awaitTermination()


  }

}
