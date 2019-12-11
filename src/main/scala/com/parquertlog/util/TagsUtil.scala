package com.parquertlog.util

import org.apache.spark.sql.Row

import org.apache.commons.lang3.StringUtils

import scala.tools.scalap.scalax.util.StringUtil

object TagsUtil {

  val oneUserID =
    """
      |imei !='' or mac!='' or idfa!='' or openudid!='' or androidid!=''
    """.stripMargin

  // 获取唯一不为空的用户ID
  def getOneUserID(row: Row): String = {

    row match {
      case v if StringUtils.isNoneEmpty(row.getAs[String]("imei")) => "IM:" + v.getAs("imei")
      case v if StringUtils.isNoneEmpty(row.getAs[String]("mac")) => "IM:" + v.getAs("mac")
      case v if StringUtils.isNoneEmpty(row.getAs[String]("idfa")) => "IM:" + v.getAs("idfa")
      case v if StringUtils.isNoneEmpty(row.getAs[String]("openudid")) => "IM:" + v.getAs("openudid")
      case v if StringUtils.isNoneEmpty(row.getAs[String]("androidid")) => "IM:" + v.getAs("androidid")
      case _ => "未知"
    }
  }


  def getAllUserID(v: Row) = {
    var userIds = List[String]()

    if (v.getAs[String]("imei").nonEmpty) userIds :+= ("IM:" + v.getAs[String]("imei").toUpperCase)
    if (v.getAs[String]("idfa").nonEmpty) userIds :+= ("DF:" + v.getAs[String]("idfa").toUpperCase)
    if (v.getAs[String]("mac").nonEmpty) userIds :+= ("MC:" + v.getAs[String]("mac").toUpperCase)
    if (v.getAs[String]("androidid").nonEmpty) userIds :+= ("AD:" + v.getAs[String]("androidid").toUpperCase)
    if (v.getAs[String]("openudid").nonEmpty) userIds :+= ("OU:" + v.getAs[String]("openudid").toUpperCase)


    userIds
  }


}
