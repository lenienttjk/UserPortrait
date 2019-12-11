package com.parquertlog.userlable

import java.io.IOException
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.parquertlog.util.TagsUtil
import com.typesafe.config.ConfigFactory
import hbasetools.HbaseTools
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Durability, HTable, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.graphx.GraphLoader


/**
  * 上下标签: 将所有标签合并
  */
object TagslableMain {
  def main(args: Array[String]): Unit = {

    val session: SparkSession = SparkSession.builder()
      .appName("Tagslable")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()


    // hbase 配置
    val load = ConfigFactory.load()
    val hbaseTableName = "sz1901"

    import org.apache.hadoop.hbase.TableName

    val configuration = session.sparkContext.hadoopConfiguration
    configuration.set("hbase.zookeeper.quorum","mini1:2181,mini2:2181,mini3:2181")
    val hbConn = ConnectionFactory.createConnection(configuration)

    //获取表的操作对象
    val admin = hbConn.getAdmin()

    if (!admin.tableExists(TableName.valueOf(hbaseTableName))) {
      println("当前表可用")
      // 创建表对象
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
      // 创建列簇
      val columnDescriptor = new HColumnDescriptor("tags")
      // 将列簇加入表中
      tableDescriptor.addFamily(columnDescriptor)
      // 创建表
      admin.createTable(tableDescriptor)
      admin.close()
      hbConn.close()
    }
    // 创建jobConf
    val jobConf =new JobConf(configuration)
    // 指定key类型
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    //指定表
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,hbaseTableName)



    val df: DataFrame = session.read.parquet("E:\\Project\\UserPortrait\\sourceData\\intpiut\\part.snappy.parquet")

    // 读取字典文件
    val lines = session.sparkContext.textFile("E:\\Project\\UserPortrait\\sourceData\\intpiut\\app_dict.txt")
    val appMap = lines.filter(_.split("\t", -1).length >= 5).map(t => {
      val fields = t.split("\t", -1)
      (fields(4), fields(1))
    }).collectAsMap()
    // 广播
    val broadcast = session.sparkContext.broadcast(appMap)


    // 读取 stopworlds
    val dataStopWorld = session.sparkContext.textFile("E:\\Project\\UserPortrait\\sourceData\\intpiut\\stopwords.txt")
    // 转换为map
    //    val kwMap = dataStopWorld.map((_, 0)).collectAsMap()
    val arr: Array[String] = dataStopWorld.collect()
    // 广播
    val broadcastSW = session.sparkContext.broadcast(arr)


    import session.implicits._

    // 过滤数据
    df.filter(TagsUtil.oneUserID).rdd

      .map(row => {


        // 1、广告标签
        val adTag = TagsAD.makeTags(row)

        //2、 商圈标签
        val businessListTag = BusinessTag.makeTags(row)

        // 3、app 标签
        val appNameTag = appTag.makeTags(row, broadcast.value)

        // 4、渠道打标签
        val channelTag = ChannelTag.makeTags(row)

        // 5、设备标签
        // 5.1 操作系统 打标签
        val operatingSystemTag = OSTag.makeTags(row)
        // 5.2 联网方式 打标签
        val netTag = NetTag.makeTags(row)
        // 5.3 运营商  打标签
        val operSystemTag = OperTag.makeTags(row)


        // 6、关键字标签
        val keyWorldTag = KeyWorldTag.makeTags(row, broadcastSW.value)

        // 7、地域标签
        val areaTag = AeraTag.makeTags(row)


        // 拿到USerID ,打标签以key 为唯一用户ID标准
        val userID = TagsUtil.getOneUserID(row)

        val userTags = adTag ++ appNameTag ++ channelTag ++
          operatingSystemTag ++ netTag ++ operSystemTag ++ keyWorldTag ++ areaTag ++ businessListTag

        // tuple(String,List(String,Int))
        (userID, userTags)


      }).reduceByKey {
      case (a, b) => {

        // 方式一                        (_ + _.2) 第一个 _为初始值0，第二个为List()的第二个值，
        //        (a ++ b).groupBy(_._1).mapValues(_.foldLeft(0)(_ + _._2)).toList
        //        (a ++ b).groupBy(_._1).mapValues(_.map(_._2).sum).toList

        // 方式二  使用偏函数
        (a ++ b).groupBy(_._1).map {
          case (k, list) => (k, list.map(_._2).sum)
        }.toList


      }
    }
            .foreach(println)

//      // 将数据写入 hbase
//      .map {
//      case (userID, userTags) => {
//        val put = new Put(Bytes.toBytes(userID))
//        put.setDurability(Durability.SKIP_WAL)
//        //设置日期格式
//        val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
//        val date = df.format(new Date())
//        //  列簇  列名 列值
//        put.addColumn(Bytes.toBytes("tags"), Bytes.toBytes(date), Bytes.toBytes(userTags.mkString(",")))
//        (new ImmutableBytesWritable, put)
//      }
//    }
//      // 将数据存入 hbase
//      .saveAsHadoopDataset(jobConf)


    session.stop()
  }
}
