package Test

import java.lang

import Utils.{JedisConnectionPool, OffSetUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
  * Author HanDong
  *
  * Date 2019/8/31 
  *
  * Description 
  **/
object Test_spark {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      // 设置每秒每个分区拉取kafka数据的速率（即：记录的条数）
      .set("spark.streaming.kafka.maxRatePerPartition", "100")
      //设置序列化方式
      .set("spark.serlizer", "org.apache.spark.serializer.KryoSerializer")

    //创建streaming的上下文对象
    val ssc = new StreamingContext(conf, Seconds(3))

    val ipRDD: RDD[String] = ssc.sparkContext.textFile("D:/Desktop/ip.txt")
    val dic_rdd: RDD[(Long, Long, String)] = ipRDD.map(x => {
      val sts: Array[String] = x.split("\\|")
      val start: Long = sts(2).toLong
      val end: Long = sts(3).toLong
      val pro: String = sts(6)
      (start, end, pro)
    })
    val bro = ssc.sparkContext.broadcast(dic_rdd.collect)

    //设置组名、topic名
    val groupId = "AAAAAA"
    val topic = "Test_A"

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

    //获取到的InputDStream
    val dStream: InputDStream[ConsumerRecord[String, String]] = OffSetUtils.getInputDStream(ssc, topics, groupId, kafkas)

    dStream.foreachRDD(rdd => {
      val line: RDD[String] = rdd.map(_.value())
      val allData: RDD[(String, String, String, String, Long)] = line.map(x => {
        val sts: Array[String] = x.split(" ")
        val time: String = sts(0)
        val ip: String = sts(1)
        val item: String = sts(2)
        val buy_its: String = sts(3)
        val money: Long = sts(4).toLong

        (time, ip, item, buy_its, money)
      })

      //问题1.计算出总的成交量总额（结果保存到Redis中）
            allData.map(t=>t._5).foreachPartition(x=>{
              val jedis: Jedis = JedisConnectionPool.getConnection()
              x.foreach(data=>{
                jedis.hincrBy("问题1","总成交量",data)
              })
              jedis.close()
            })


      //问题2.计算每个商品分类的成交量的（结果保存到Redis中）
            allData.map(t=>(t._3,t._5)).reduceByKey(_+_).foreachPartition(x=>{
              val jedis: Jedis = JedisConnectionPool.getConnection()
              x.foreach(data=>{
                jedis.hincrBy("问题2",data._1,data._2)
              })
              jedis.close()
            })

      //问题3.计算每个地域的商品成交总金额（结果保存到Redis中）
      allData.map(t => (t._2, t._5)).reduceByKey(_ + _).foreachPartition(x => {
        val jedis: Jedis = JedisConnectionPool.getConnection()
        x.foreach(data => {
          val ip_ten: Long = ip2Long(data._1)
          val province: String = ErSerach(bro.value, ip_ten)
          jedis.hincrBy("问题3", province, data._2)
        })
        jedis.close()
      })


    })
    ssc.start()
    ssc.awaitTermination()
  }

  // 将IP转换成十进制
  def ip2Long(ip: String): Long = {
    val s = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until s.length) {
      ipNum = s(i).toLong | ipNum << 8L
    }
    ipNum
  }

  //二分查找
  def ErSerach(arr: Array[(Long, Long, String)], key: Long): String = {

    var l = 0
    var h = arr.length
    while (h >= l) {
      var m = (l + h) / 2
      if ((key > arr(m)._1) && (key < arr(m)._2)) {
        return arr(m)._3
      }
      else if (key < arr(m)._1) {
        h = m - 1
      }
      else if (key > arr(m)._2) {
        l = m + 1
      }
    }
    "查找失败"
  }
}