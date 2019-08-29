package Work

import java.sql.{Connection, Statement}

import Utils.{JedisConnectionPool, MysqlPoolUtils}
import org.apache.spark.rdd.RDD
import redis.clients.jedis.Jedis

/**
  * Author HanDong
  *
  * Date 2019/8/28 
  *
  * Description 
  **/
object resultShow {
  def workSurvey1_1(ds: RDD[(String, List[Double], (String, String), String,String)]) = {
    //先map输出一个阶段 只取需要的2个字段
    val res: RDD[(String, List[Double])] = ds.map(x => (x._1, x._2))
    val rdd: RDD[(String, List[Double])] = res.reduceByKey((list1, list2) => {
      list1.zip(list2).map(x => x._1 + x._2)
    })

    rdd.foreachPartition(line => {
      line.foreach(x => {
        //          println(x._1+" "+x._2(0)+" "+x._2(1)+"  "+x._2(2))
        val jedis: Jedis = JedisConnectionPool.getConnection()
        jedis.hincrBy("Deposit_Control", "订单数", x._2(0).toLong)
        jedis.hincrByFloat("Deposit_Control", "充值金额", x._2(1))
        jedis.hincrBy("Deposit_Control", "充值成功数", x._2(2).toLong)
        println("process1 ing````")
        jedis.close()

      })
    })
  }

  def workSurvey1_2(ds: RDD[(String, List[Double], (String, String), String,String)]) = {
    val res: RDD[(String, List[Double])] = ds.map(x => (x._1, x._2))
    val rdd: RDD[(String, List[Double])] = res.reduceByKey((list1, list2) => {
      list1.zip(list2).map(x => x._1 + x._2)
    })

    rdd.foreachPartition(line => {
      line.foreach(x => {
        //          println(x._1+" "+x._2(0)+" "+x._2(1)+"  "+x._2(2))
        val jedis: Jedis = JedisConnectionPool.getConnection()
        jedis.hincrBy("Deposit_Control", x._1, x._2(0).toLong)
        //        jedis.hincrByFloat("Deposit_Control", "充值金额", x._2(1))
        //        jedis.hincrBy("Deposit_Control", "充值成功数", x._2(2).toLong)
        println("process2 ing````")
        jedis.close()

      })
    })
  }


  def workSurvey2(ds: RDD[(String, List[Double], (String, String), String,String)]) = {
    val res: RDD[((String, String), List[Double])] = ds.map(x => (x._3, x._2))

    val rdd: RDD[((String, String), List[Double])] = res.reduceByKey((list1, list2) => {
      list1.zip(list2).map(x => x._1 + x._2)
    })

    rdd.foreachPartition(line => {
      val conn: Connection = MysqlPoolUtils.getConnection.get
      line.foreach(x=>{
        val sql = "insert into survey2 values('"+x._1._1+"','"+x._1._2+"','"+ (x._2.head-x._2(2)).toLong +"')"
        val statement: Statement = conn.createStatement()
        statement.executeUpdate(sql)
      })
      conn.close()


//      val jedis: Jedis = JedisConnectionPool.getConnection()
//      line.foreach(x => {
//        jedis.hincrBy(x._1._1, x._1._2, x._2.head.toLong - x._2(2).toLong)
//        println("process3 ing````")
//
//      })
//      jedis.close()

    })

  }

  def workSurvey3(ds: RDD[(String, List[Double], (String, String), String,String)]) = {
    val res: RDD[(String, List[Double])] = ds.map(x => (x._4, x._2))


    val rdd: RDD[(String, List[Double])] = res.reduceByKey((list1, list2) => {
      list1.zip(list2).map(x => x._1 + x._2)
    })
    //    rdd.sortBy(_._2(0),false)
    //        .map(t=>(t._1,t._2(0),(t._2(2)/t._2(0) * 100).formatted("%.1f"))).take(10)
    //        .foreach(x=>{
    //          println(x)
    //        })
    rdd.sortBy(_._2.head, false)
      .foreachPartition(line => {
        val conn: Connection = MysqlPoolUtils.getConnection.get
        line.foreach(x => {
          val sql = "insert into survey3 values('" + x._1 + "','" + x._2(0) + "','" + (x._2(2) / x._2(0) * 100).formatted("%.1f") + "')"
          //          println(x._1 + " " + x._2(0) + "  " + (x._2(2) / x._2(0) * 100).formatted("%.1f"))
          val statement: Statement = conn.createStatement()
          statement.executeUpdate(sql)
        })
        conn.close()
      })



    //    rdd.foreachPartition(line => {
    //
    //
    //
    //      line.foreach(x => {
    //        println(x._1 + " " + x._2(0) + "  " + x._2(2) / x._2(0))
    //      })
    //    })
  }
  def workSurvey4(ds:RDD[(String,List[Double])])={
    ds.foreachPartition(line=>{
      val conn: Connection = MysqlPoolUtils.getConnection.get
      line.foreach(x=>{
        val sql = "insert into survey4 values('"+x._1+"','"+x._2(0)+"','"+x._2(1)+"')"
        val statement: Statement = conn.createStatement()
        statement.executeUpdate(sql)
      })
      conn.close()
    })

  }

}
