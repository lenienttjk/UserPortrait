package com.parquertlog.luanch

import scala.collection.mutable.ListBuffer

/**
  * 业务处理类
  */
object AeraUtil {

  // 封装成List[double](n,n,n)  原始请求，有效请求，广告请求

  // requestmode =1 	processnode >=1
  def caculateReq(req: Int, pro: Int) = {
    if (req == 1 && pro >= 1) {
      List[Double](1, 0, 0)
    } else if (req == 1 && pro >= 2) {
      List[Double](1, 1, 0)
    } else if (req == 1 && pro >= 3) {
      List[Double](1, 1, 1)
    } else
      List[Double](0, 0, 0)

  }


  // 封装成 List（） iseffective  isbilling
  def caculateRtb(effTive: Int, billing: Int, bid: Int, adorderid: Int, win: Int, winPrice: Double, adPayMent: Double) = {

    if (effTive == 1 && billing == 1 && bid == 1 && adorderid != 0) {
      List[Double](1, 0, 0, 0)
    } else if (effTive == 1 && billing == 1 && win == 1) {
      List[Double](0, 1, winPrice / 1000.0, adPayMent / 1000.0)
    } else {
      List[Double](0, 0, 0, 0)
    }
  }


  //  List(广告展示，点击)
  def caculaateShowClick(reqMode: Int, effTive: Int) = {
    if (reqMode == 1 && effTive == 1) {
      List[Double](1, 0)
    } else if (reqMode == 3 && effTive == 1) {
      List[Double](0, 1)
    } else
      List[Double](0, 0)
  }


  // 一次 传9个参数
  def reqAd(req: Int, pro: Int, effTive: Int, billing: Int, bid: Int, win: Int, adorderid: Int,winPrice: Double, adPayMent: Double) = {

    // 处理请求
    var list1 = ListBuffer[Double]()
    if (req == 1 && pro >= 1) {
      list1 += (1, 0, 0)
    } else if (req == 1 && pro >= 2) {
      list1 += (1, 1, 0)
    } else if (req == 1 && pro >= 3) {
      list1 += (1, 1, 1)
    } else {
      list1 += (0, 0, 0)
    }

    // 处理竞价,成本
    var list2 = ListBuffer[Double]()
    if (effTive == 1 && billing == 1 && bid == 1 && adorderid != 0) {
      list2 += (1, 0, 0, 0)
    } else if (effTive == 1 && billing == 1 && win == 1) {
      list2 += (0, 1, winPrice / 1000.0, adPayMent / 1000.0)
    } else {
      list2 += (0, 0, 0, 0)
    }


    // 处理展示，点击
    var list3 = ListBuffer[Double]()
    if (req == 1 && effTive == 1) {
      list3 += (1, 0)
    } else if (req == 3 && effTive == 1) {
      list3 += (0, 1)
    } else
      list3 += (0, 0)

     // 返回
    list1 ++ list2 ++ list3

  }


}
