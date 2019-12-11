package com.parquertlog.userlable

import ch.hsr.geohash.GeoHash
import com.parquertlog.util.{LableTrait, StrUtils, redisCluster}
import org.apache.spark.sql.Row

object BusinessTag extends LableTrait {



  override def makeTags(args: Any*): List[(String, Int)] = {

    var list = List[(String, Int)]()

    val row = args(0).asInstanceOf[Row]

    // 获取经纬度
    val lag = row.getAs[String]("long")
    val lat = row.getAs[String]("lat")


    // 获取商圈
    val business = getBusiness(lag, lat)
    val lines = business.split(",")
    lines.foreach(t => {
      list :+= (t, 1)
    })

    list
  }


  // 使用GeoHash 获取商圈
  def getBusiness(lag: String, lat: String) = {

    // 获取 6 位 GeoHash
    val geoHash = GeoHash.geoHashStringWithCharacterPrecision(StrUtils.toDouble(lat), StrUtils.toDouble(lag), 6)

    // 先去数据库查，然后查不到，再去http 高德地图查
    val jedis = redisCluster.jedis
    var str = jedis.get(geoHash)

    // 判断
    if (null == str || 0 == str.length) {
      str = AeramapUtil.getBusinessFromAeramap(StrUtils.toDouble(lag),StrUtils.toDouble(lat))
      // 保存
      jedis.set(geoHash, str)
    }

    str
  }




}
