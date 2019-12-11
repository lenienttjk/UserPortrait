package com.parquertlog.userlable


import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

import scala.collection.mutable.ListBuffer


/**
  * 从地图获取信息
  */
object AeramapUtil {

  def getUrl(urlStr: String) = {
    // 获取 客户端
    val client = HttpClients.createDefault()
    val httpGet = new HttpGet(urlStr)

    // 发送请求
    val reponse = client execute (httpGet)
    // 格式化结果
    EntityUtils.toString(reponse.getEntity, "UTF-8")

  }

  def getBusinessFromAeramap(lag: Double, lat: Double): String = {

    // 拼接 经纬度
    val location = lag + "," + lat

    // 获取 url
    val urlStr = "https://restapi.amap.com/v3/geocode/regeo?location=" +location+ "&key=d45d9706da166e7d6b2e71d6fa5d23c5"

    // 获取http，返回的是json
    val json = getUrl(urlStr)

    // 解析 json
    val jsonObj = JSON.parseObject(json)
    // 判断状态
    val status = jsonObj.getIntValue("status")
    if (0 == status) {
      return ""
    }

    //
    val regeocogeJson = jsonObj.getJSONObject("regeocoge")
    if(regeocogeJson == null || regeocogeJson.keySet().isEmpty){
      return ""
    }

    val addJson = regeocogeJson.getJSONObject("addressComponent")
    if(addJson == null || addJson.keySet().isEmpty){
      return ""
    }


    val arr = addJson.getJSONArray("businessAreas")
    if(arr == null || arr.isEmpty){
      return ""
    }


    var list = ListBuffer[String]()
    for (item <- arr.toArray) {

      if (item.isInstanceOf[JSONObject]) {
        val json = item.asInstanceOf[JSONObject]
        val businessName = json.getString("name")
        list.append(businessName)
      }

    }
    list.mkString(",")
  }

}
