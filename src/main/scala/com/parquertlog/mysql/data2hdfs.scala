package com.parquertlog.mysql

import java.sql.SQLException

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  *  最终生成一个 分区的文件
  *     在RDD上调用coalesce(1,true).saveAsTextFile()
  *     repartition(1)
  */
object data2hdfs {

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .appName("data2hdfs")
      .master("local")
      .getOrCreate()


    val df: DataFrame = session.read.parquet("hdfs://mini1:9000/input/data.parquet")


    val result: DataFrame = df.select("provincename", "cityname").groupBy("provincename", "cityname").count()


    df.createTempView("v_parquert")

    val df2 = session.sql("select  provincename, cityname,count(cityname) as counts from v_parquert group by provincename,cityname order by counts desc")


    // 使用  partitionBy 可以使用 列名作为 目录

    df2.write.partitionBy("provincename","cityname").json("hdfs://mini1:9000/")


    session.stop()
  }


}
