package Utils

import Consumer.JedisOffset
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
/**
  * Author HanDong
  *
  * Description
  **/

object OffSetUtils {
  def getInputDStream(ssc:StreamingContext,
                      topics: Set[String],
                      groupId: String,
                      kafkas: Map[String, Object]): InputDStream[ConsumerRecord[String,String]] ={
    // 第一步获取Offset
    // 第二步通过Offset获取Kafka数据
    // 第三步提交更新Offset
    // 获取Offset
    var fromOffset:Map[TopicPartition,Long] = JedisOffset(groupId)
    // 判断一下有没数据
    val stream :InputDStream[ConsumerRecord[String,String]] =
      if(fromOffset.size == 0){
        KafkaUtils.createDirectStream(ssc,
          // 本地策略
          // 将数据均匀的分配到各个Executor上面
          LocationStrategies.PreferConsistent,
          // 消费者策略
          // 可以动态增加分区
          ConsumerStrategies.Subscribe[String, String](topics,kafkas)
        )
      }else{
        // 不是第一次消费
        KafkaUtils.createDirectStream(
          ssc,
          LocationStrategies.PreferConsistent,
          ConsumerStrategies.Assign[String,String](fromOffset.keys,kafkas,fromOffset)
        )
      }
    stream
  }

  def saveOfferToRedis(rdd: RDD[ConsumerRecord[String, String]], groupId: String): Unit ={
    val offestRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    val jedis = JedisConnectionPool.getConnection()
    for (or<-offestRange){
      jedis.hset(groupId,or.topic+"-"+or.partition,or.untilOffset.toString)
    }
    jedis.close()
  }



}
