package com.parquertlog.mysql

import java.sql.SQLException

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object data2json {

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .appName("data2mysql")
      .master("local")
      //      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()


    val df: DataFrame = session.read.parquet("E:\\Project\\UserPortrait\\sourceData\\intpiut\\part.snappy.parquet")

      df.show(false)

    val result: DataFrame = df.select("provincename", "cityname").groupBy("provincename", "cityname").count()


    df.createTempView("v_parquert")
    val df2 = session.sql("select  provincename, cityname,count(cityname) as counts from v_parquert group by provincename,cityname order by counts desc")

    // 将数据转换成 json
//    df2.write.json("E:\\Project\\UserPortrait\\sourceData\\json")




    session.stop()
  }


}
