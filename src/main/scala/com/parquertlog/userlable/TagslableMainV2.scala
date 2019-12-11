package com.parquertlog.userlable

import java.text.SimpleDateFormat
import java.util.Date

import com.parquertlog.util.TagsUtil
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Durability, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 利用图计算，进行统一用户身份的数据合并
  */
object TagslableMainV2 {
  def main(args: Array[String]): Unit = {

    val session: SparkSession = SparkSession.builder()
      .appName("TagslableMainV2")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()


    // hbase 配置
    val load = ConfigFactory.load()
    val hbaseTableName = "sz1901"

    import org.apache.hadoop.hbase.TableName

    val configuration = session.sparkContext.hadoopConfiguration
    configuration.set("hbase.zookeeper.quorum", "mini1:2181,mini2:2181,mini3:2181")
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
    val jobConf = new JobConf(configuration)
    // 指定key类型
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    //指定表
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTableName)


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

    //  标签  (userId, row)
    val baseRDD = df.filter(TagsUtil.oneUserID)
      .rdd
      .map(row => {
        val userId = TagsUtil.getAllUserID(row)
        //  userId 为 List(String,String...) 多个
        (userId, row)
      })

    // 图计算：构建点
    val VD = baseRDD.flatMap(r => {
      val row = r._2
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

      val userTags = adTag ++ appNameTag ++ channelTag ++
        operatingSystemTag ++ netTag ++ operSystemTag ++ keyWorldTag ++ areaTag ++ businessListTag

      val VD = r._1.map((_, 0)) ++ userTags

      r._1.map(uid => {
        if (r._1.head.equals(uid)) {
          (uid.hashCode.toLong, VD)
        } else {
          (uid.hashCode.toLong, List.empty)
        }
      })
    })

    // 构建边
    val ED = baseRDD.flatMap(tp => {
      //  A B C   A->B  A -> C   保证 A 不变
      tp._1.map(uid => {
        // Edge(n1,n2,n3) 三个参数
        Edge(tp._1.head.hashCode.toLong, uid.hashCode.toLong,0)
      })
    })

    // 构建图
    val graph = Graph(VD, ED)

    // 取出顶点
    val vertices = graph.connectedComponents().vertices

    //
    vertices.join(VD).map {
      case (uid, (vd, tagsUserId)) => (vd, tagsUserId)
    }
      .reduceByKey {
        case (list1, list2) =>
          (list1 ++ list2).groupBy(_._1)
            //          .mapValues(_.foldLeft(0)(_ + _._2)).toList
            .mapValues(_.map(_._2).sum).toList
      }
      // 将数据写入 hbase
      .map {
      case (userID, userTags) => {
        val put = new Put(Bytes.toBytes(userID))
        put.setDurability(Durability.SKIP_WAL)
        //设置日期格式
        val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val date = sdf.format(new Date())
        //  列簇  列名 列值
        put.addColumn(Bytes.toBytes("tags"), Bytes.toBytes(date), Bytes.toBytes(userTags.mkString(",")))
        (new ImmutableBytesWritable, put)
      }
    }
      // 将数据存入 hbase
      .saveAsHadoopDataset(jobConf)


  }
}
