package com.parquertlog.luanch

import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *  1 地域指标 省份，城市
  */

object AeraTarget {
  def main(args: Array[String]): Unit = {

    val session: SparkSession = SparkSession.builder()
      .appName("Aera")
      .master("local[*]")
      //      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()


    val df: DataFrame = session.read.parquet("E:\\Project\\UserPortrait\\sourceData\\intpiut\\part.snappy.parquet")

    import session.implicits._
    // 获取字段  从dataFrame

    df.rdd.map(t => {
      val provincename = t.getAs[String]("provincename")
      val cityname = t.getAs[String]("cityname")

      val requestmode = t.getAs[Int]("requestmode")
      val processnode = t.getAs[Int]("processnode")

      val iseffective = t.getAs[Int]("iseffective")
      val isbilling = t.getAs[Int]("isbilling")
      val isbid = t.getAs[Int]("isbid")
      val iswin = t.getAs[Int]("iswin")
      val adorderid = t.getAs[Int]("adorderid")

      val winprice = t.getAs[Double]("winprice")
      val adPayMent = t.getAs[Double]("adpayment")

      // 转换为List
      // (effTive: Int, billing: Int, bid: Int, win: Int, winPrice: Int, adorderid: Int, adPayMent: Double)
      val reqList = AeraUtil.caculateReq(requestmode, processnode)
      val rtpList = AeraUtil.caculateRtb(iseffective, isbilling, isbid, adorderid, iswin, winprice, adPayMent)
      val showclickList = AeraUtil.caculaateShowClick(requestmode, iseffective)


      // 返回值 (key,value)
      ((provincename, cityname), reqList ++ rtpList ++ showclickList)

    }).reduceByKey((list1, list2) => {
      // zip 操作 拉到一起成为一个 tuple ( Double,Doube)  , 再 map 用+ 连接符 变成 一个 Double
      list1.zip(list2).map(t => t._1 + t._2)

      // t._1._1 即 provincename    t._1._2 即 cityname
    }).map(t => t._1._1 + "," + t._1._2 + "," + t._2.mkString(","))

//      .saveAsTextFile("")
        .foreach(println)
    session.stop()
  }
}
