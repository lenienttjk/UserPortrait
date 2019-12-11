package com.parquertlog.userlable

import com.parquertlog.util.LableTrait
import org.apache.spark.sql.Row

object TagsAD extends LableTrait {

  override def makeTags(args: Any*): List[(String, Int)] = {

    var list = List[(String, Int)]()

    val row = args(0).asInstanceOf[Row]


    // 获取广告类型ID
    val adTypeID = row.getAs[Int]("adspacetype")
    adTypeID match {
      case v if v>9 =>list:+=("LC"+v,1)
      case v if v>0 && v <=9 =>list:+=("LC0"+v,1)
    }

    // 获取广告IDname
    val adName = row.getAs[String]("adspacetypename")
    if (!adName.isEmpty) {
      list :+= ("LN" + adName, 1)
    }

    list

  }
}
