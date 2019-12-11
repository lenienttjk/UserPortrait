package com.parquertlog.luanch

import com.parquertlog.util.{ redisCluster}
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * 2  媒体分析 appname 指标  使用 redis
  */
object AppTargetRedis {
  def main(args: Array[String]): Unit = {

    val session: SparkSession = SparkSession.builder()
      .appName("AppTargetRedis")
      .master("local[*]")
      //      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()


    val df: DataFrame = session.read.parquet("E:\\Project\\UserPortrait\\sourceData\\intpiut\\part.snappy.parquet")


    // 读取字典文件
    val lines = session.sparkContext.textFile("E:\\Project\\UserPortrait\\sourceData\\intpiut\\app_dict.txt")


    //  一个分区的写进入
    val appMap = lines.filter(_.split("\t", -1).length >= 5)
      .map(t => {
        val fields = t.split("\t", -1)
        (fields(4), fields(0))
      })

    appMap.foreachPartition(t => {
      t.foreach(t => {
        val jedis = redisCluster.jedis
        jedis.set(t._1, t._2)
      })
    })



    // 获取字段  从dataFrame
    df.rdd.mapPartitions(f => {

      f.map(t => {
        val appid = t.getAs[String]("appid")
        var appname = t.getAs[String]("appname")
        if (appname.isEmpty) {

          // 获取连接  读取 redis 数据
          val jedis = redisCluster.jedis
          appname = jedis.get(appid)

        }


        val requestmode = t.getAs[Int]("requestmode")
        val processnode = t.getAs[Int]("processnode")

        val iseffective = t.getAs[Int]("iseffective")
        val isbilling = t.getAs[Int]("isbilling")
        val isbid = t.getAs[Int]("isbid")
        val iswin = t.getAs[Int]("iswin")
        val adorderid = t.getAs[Int]("adorderid")

        val winprice = t.getAs[Double]("winprice")
        val adPayMent = t.getAs[Double]("adpayment")

        // 返回值 (key,value)
        ((appname), AeraUtil.reqAd(requestmode, processnode, iseffective, isbilling, isbid, iswin, adorderid, winprice, adPayMent))
      })


    }).reduceByKey((list1, list2) => {
      // zip 操作 拉到一起成为一个 tuple ( Double,Doube)  , 再 map 用+ 连接符 变成 一个 Double ,(Double+Double)
      list1.zip(list2).map(t => t._1 + t._2)
    }).map(t => t._1 + " ： " + t._2.mkString(","))
      .foreach(println)



    //      .reduceByKey((list1,list2)=>{
    //        // list1(1,2,3,4) list2(1,2,3,4) zip(List((1,1),(2,2),(3,3),(4,4)))
    //        list1.zip(list2)
    //          // List((1+1),(2+2),(3+3),(4+4))
    //          .map(t=>t._1+t._2)
    //        // List(2,4,6,8)
    //      }).map(t=>t._1+" : "+t._2.mkString("<",",",">")).foreach(println)
    //


    session.stop()
  }
}
