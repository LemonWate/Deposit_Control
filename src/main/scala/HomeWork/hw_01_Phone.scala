package HomeWork

import java.lang
import java.util.Date

import Utils.{JedisConnectionPool, OffSetUtils, timeUtils}
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
  * Author HanDong
  *
  * Date 2019/8/29 
  *
  * Description
  * 1.求每天充值总金额
  * 2.求每月各手机号充值的平均金额
  **/
object hw_01_Phone {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      // 设置每秒每个分区拉取kafka数据的速率（即：记录的条数）
      .set("spark.streaming.kafka.maxRatePerPartition", "100")
      //设置序列化方式
      .set("spark.serlizer", "org.apache.spark.serializer.KryoSerializer")

    //创建streaming的上下文对象
    val ssc = new StreamingContext(conf, Seconds(3))

    //设置组名、topic名
    val groupId = "hw_group1"
    val topic = "JsonData"

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

    dStream.foreachRDD(rdd => {
  val allData: RDD[JSONObject] = rdd.map(_.value()).map(JSON.parseObject(_))

      val resRDD: RDD[(String, Double, String)] = allData.map(x => {
        val phone: String = x.getString("phoneNum")
        val money: Double = x.getString("money").toDouble
//        val date: String = timeUtils.formatTime(x.getString("date"))
        val time: String = x.getString("date").substring(0, 10)
        (phone, money, time)
      })

      //求每天充值总金额
      resRDD.map(x=>(x._3,x._2)).reduceByKey(_+_).foreachPartition(t=>{
        val jedis: Jedis = JedisConnectionPool.getConnection()
        t.foreach(line=>{
          jedis.hincrBy(topic,line._1,line._2.toLong)
        })
        jedis.close()
      })
      //求每月各手机号充值的平均金额
      resRDD.map(x=>(x._3.substring(0,7),(x._1,x._2,1))).foreachPartition(t=>{
        val jedis: Jedis = JedisConnectionPool.getConnection()
        t.foreach(line=>{
          jedis.hincrBy(line._1,line._2._1,(line._2._2/line._2._3).toLong)
        })
        jedis.close()
      })






    })
    ssc.start()
    ssc.awaitTermination()

  }

}
