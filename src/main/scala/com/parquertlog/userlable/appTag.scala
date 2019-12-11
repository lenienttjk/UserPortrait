package com.parquertlog.userlable

import com.parquertlog.util.LableTrait
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row


/**
  *  APP 打 标签
 */
object appTag extends LableTrait {
  override def makeTags(args: Any*): List[(String, Int)] = {

    var list = List[(String, Int)]()

    val row = args(0).asInstanceOf[Row]
    val bro = args(1).asInstanceOf[collection.Map[String, String]]


    // 获取 appName
    val appid = row.getAs[String]("appid")
    var appname = row.getAs[String]("appname")
    if (!appname.isEmpty) {
      list :+= ("APP" + appname, 1)
    }else if (!appid.isEmpty) {
      appname = bro.getOrElse(appid, "其他")
      list :+= ("APP" + appname, 1)
    }

    list
  }
}
