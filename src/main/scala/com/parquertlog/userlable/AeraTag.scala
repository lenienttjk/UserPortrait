package com.parquertlog.userlable

import org.apache.commons.lang3.StringUtils
import com.parquertlog.util.LableTrait
import org.apache.spark.sql.Row

object AeraTag extends LableTrait {


  override def makeTags(args: Any*): List[(String, Int)] = {

    var list = List[(String, Int)]()

    val row = args(0).asInstanceOf[Row]

    //地域指标
    val pName = row.getAs[String]("provincename")
    val cName = row.getAs[String]("cityname")

    if (StringUtils.isNotEmpty(pName)) {
      list :+= ("ZP" + pName, 1)
    }

    if (StringUtils.isNotEmpty(cName)) {
      list :+= ("ZP" + cName, 1)
    }

    list
  }
}
