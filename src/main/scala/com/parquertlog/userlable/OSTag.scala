package com.parquertlog.userlable

import com.parquertlog.util.LableTrait
import org.apache.spark.sql.Row

object OSTag  extends LableTrait{
  override def makeTags(args: Any*): List[(String, Int)] = {

    var list = List[(String, Int)]()

    val row = args(0).asInstanceOf[Row]

    // 获取  操作系统
    val clientID = row.getAs[Int]("client")

    clientID match {
      case v if v ==1  =>list:+=("1 Android D00010001",1)
      case v if v ==2 =>list:+=("2 IOS D00010002",1)
      case v if v ==3 =>list:+=("3 WinPhone D00010003",1)
      case _ =>list:+=("4 其他 D00010004",1)
    }
    list

  }


}
