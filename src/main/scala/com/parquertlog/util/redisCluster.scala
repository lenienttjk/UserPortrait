package com.parquertlog.util

import redis.clients.jedis.HostAndPort
import redis.clients.jedis.JedisCluster
import java.util

import org.apache.commons.pool2.impl.GenericObjectPoolConfig

//  object 类似java 的 static
object redisCluster {


  // 添加集群的服务节点Set集合
  val hostAndPorts = new util.HashSet[HostAndPort]()

  // 添加节点
  hostAndPorts.add(new HostAndPort("mini1", 7001))
  hostAndPorts.add(new HostAndPort("mini1", 7002))
  hostAndPorts.add(new HostAndPort("mini1", 7003))
  hostAndPorts.add(new HostAndPort("mini1", 7004))
  hostAndPorts.add(new HostAndPort("mini1", 7005))
  hostAndPorts.add(new HostAndPort("mini1", 7006))

  // Jedis连接池配置
  val jedisPoolConfig = new GenericObjectPoolConfig()
  // 最大空闲连接数, 默认8个
  jedisPoolConfig.setMaxIdle(100)
  // 最大连接数, 默认8个
  jedisPoolConfig.setMaxTotal(100)
  //最小空闲连接数, 默认0
  jedisPoolConfig.setMinIdle(10)
  // 获取连接时的最大等待毫秒数(如果设置为阻塞时BlockWhenExhausted),如果超时就抛异常, 小于零:阻塞不确定的时间,  默认-1
  jedisPoolConfig.setMaxWaitMillis(2000); // 设置2秒
  //对拿到的connection进行validateObject校验
  jedisPoolConfig.setTestOnBorrow(true)

  // 连接
  val jedis = new JedisCluster(hostAndPorts, 2000, 100, 100, jedisPoolConfig)

}



//object redisCluster {
//  def main(args: Array[String]): Unit = {
//
//    // 获取key的值
//    val res1 = JedisClusterPool.jedis.get("FilterChangeFlag")
//    // 输出
//    println(res1)
//    JedisClusterPool.jedis.close()
//  }
//}