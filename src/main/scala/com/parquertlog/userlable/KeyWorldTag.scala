package com.parquertlog.userlable

import com.parquertlog.util.LableTrait
import org.apache.spark.sql.Row

import scala.collection.mutable

object KeyWorldTag extends LableTrait {
  override def makeTags(args: Any*): List[(String, Int)] = {

    var list = List[(String, Int)]()

    val row = args(0).asInstanceOf[Row]

    // 获取   关键字
    val kms = row.getAs[String]("keywords")

    // stop Word
    val stopWords = args(1).asInstanceOf[Array[String]]


    kms.split("\\|")
      .filter(km => km.length >= 3 && km.length <= 8 && !stopWords.contains(km))
      .foreach(km => {
        list :+= ("K" + km, 1)
      })


    list

  }
}
