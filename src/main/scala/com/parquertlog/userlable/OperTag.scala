package com.parquertlog.userlable

import com.parquertlog.util.LableTrait
import org.apache.spark.sql.Row

object OperTag  extends LableTrait{
  override def makeTags(args: Any*): List[(String, Int)] = {

    var list = List[(String, Int)]()

    val row = args(0).asInstanceOf[Row]

    /*
    备运营商方式
移动 D00030001 联通 D00030002 电信 D00030003
_ D00030004
     */

    // 获取  运营商
    val ispnameID = row.getAs[String]("ispname")

    ispnameID match {
      case v if v =="移动"  =>list:+=(v+" D00030001",1)
      case v if v =="联通" =>list:+=(v+" D00030002",1)
      case v if v =="电信" =>list:+=(v+" D00030003",1)
      case _ =>list:+=("D00030004",1)
    }
    list

  }
}
