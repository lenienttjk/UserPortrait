package com.parquertlog.userlable

import com.parquertlog.util.LableTrait
import org.apache.spark.sql.Row

object NetTag  extends LableTrait{
  override def makeTags(args: Any*): List[(String, Int)] = {

    var list = List[(String, Int)]()

    val row = args(0).asInstanceOf[Row]

    // 获取  联网方式
    val networkmannernameID = row.getAs[String]("networkmannername")

    networkmannernameID.toUpperCase() match {
      case v if v =="WIFI"  =>list:+=("WIFI D00020001",1)
      case v if v =="4G"=>list:+=("4G D00020002",1)
      case v if v =="3G" =>list:+=("3G D00020003",1)
      case v if v =="2G" =>list:+=("2G D00020004",1)
      case _ =>list:+=("D00010005",1)
    }
    list

  }
}
