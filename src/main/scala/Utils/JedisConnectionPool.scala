package Utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * Author HanDong
  *
  * Date 2019/8/23 
  *
  * Description 
  **/
object JedisConnectionPool {
  //连接配置
  val conf = new JedisPoolConfig
  //最大连接数
  conf.setMaxTotal(20)
  //最大空闲连接
  conf.setMaxIdle(10)
  //设置连接池属性分别有： 配置  主机名   端口号  连接超时时间    Redis密码
  val pool = new JedisPool(conf,"192.168.189.233",6379,10000)
  //连接池
  def getConnection():Jedis ={
    pool.getResource
  }
}
