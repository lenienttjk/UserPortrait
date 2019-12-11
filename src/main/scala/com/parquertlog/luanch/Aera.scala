package com.parquertlog.luanch

import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  *
  */
object Aera {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .appName("Aera")
      .master("local[*]")
      //      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()


    val df: DataFrame = session.read.parquet("E:\\Project\\UserPortrait\\sourceData\\out\\part.snappy.parquet")

//    df.show(false)

    df.createTempView("v_log")

    val res = session.sql(
      """
        |select
        |provincename, cityname,
        |sum(case when requestmode = 1 and processnode >= 1 then 1 else 0 end) as Originalrequest,
        |sum(case when requestmode = 1 and processnode >= 2 then 1 else 0 end) as Validrequest,
        |sum(case when requestmode = 1 and processnode >= 3 then 1 else 0 end) as adrequest,
        |sum(case when iseffective = 1 and isbilling = 1 and isbid =1 then 1 else 0 end) as Participationauction,
        |sum(case when iseffective = 1 and isbilling = 1  and iswin =1 and adorderid != 0 then 1 else 0 end) as Auctionsuccess,
        |sum(case when requestmode = 2 and iseffective = 1 then 1 else 0 end) as showNum,
        |sum(case when requestmode = 3 and iseffective  = 1 then 1 else 0 end) as clickNum,
        |sum(case when iseffective = 1 and isbilling  = 1 and iswin = 1 then 1.0*adpayment/1000 else 0 end) as Adcosts,
        |sum(case when iseffective = 1 and isbilling  = 1 and iswin = 1 then 1.0*winprice/1000 else 0 end) as Adconsumption
        |from v_log
        |group by provincename, cityname
        |order by Originalrequest,Validrequest desc
      """.stripMargin
    )
        res.show(false)

  }
}
