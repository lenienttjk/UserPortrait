package com.parquertlog.userlable

import com.parquertlog.util.LableTrait
import org.apache.spark.sql.Row

/**
  *  渠道 打标签
  */
object ChannelTag extends  LableTrait{
  override def makeTags(args: Any*): List[(String, Int)] = {


    var list = List[(String, Int)]()

    val row = args(0).asInstanceOf[Row]

    // 获取 渠道ID
    val adplatformproviderid = row.getAs[Int]("adplatformproviderid")
    if ( adplatformproviderid.toString != null) {
      list :+= ("CN" + adplatformproviderid, 1)
    }
    list

  }
}
