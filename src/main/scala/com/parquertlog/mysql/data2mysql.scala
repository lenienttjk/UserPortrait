package com.parquertlog.mysql

import java.sql.SQLException
import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object data2mysql {

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .appName("data2mysql")
      .master("local")
      //      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()


    val df: DataFrame = session.read.parquet("E:\\Project\\UserPortrait\\sourceData\\out\\part.snappy.parquet")


    //    df.show()


    val result: DataFrame = df.select("provincename", "cityname").groupBy("provincename", "cityname").count()

    //        result.show(1000)


    df.createTempView("v_parquert")

    val df2 = session.sql("select  provincename, cityname,count(cityname) as counts from v_parquert group by provincename,cityname order by counts desc")

    val load = ConfigFactory.load()
    val properties = new Properties()
    properties.setProperty("user",load.getString("jdbc.username"))
    properties.setProperty("passwd",load.getString("jdbc.passwd"))
    df2.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"),"proandcity",properties)

    //    df2.show(1000)
    val res = df2.rdd


    //保存到mysql数据库
    res.foreachPartition(f => {
      // 将数据存储到MySQL数据库
      val connection = JDBCUtill.getConnection
      // 加载SQL语句
      val pstmt = connection.prepareStatement("insert into cityCount(provincename,cityname,ct) values(?,?,?)")
      // 写入数据
      try {
        f.foreach(t => {
          pstmt.setString(1, t.get(0).toString)
          pstmt.setString(2, t.get(1).toString)
          pstmt.setString(3, t.get(2).toString)
          val i = pstmt.executeUpdate()
          if (i > 0) println("写入成功") else println("写入失败")
        })
      } catch {
        case e: SQLException => println(e.getMessage)
      } finally {
        pstmt.close()
        connection.close()
      }
    })


    session.stop()
  }


}
