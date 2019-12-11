/**
 * @Description
 * @Author cqh <caoqingghai@1000phone.com>
 * @Version V1.0
 * @Since 1.0
 * @Date 2019/9/9 15：47
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 获取HDFS的客户端进行增删改查的操作
 */
public class HdfsClient {
    FileSystem fs = null;

    /**
     * 获取HDFS文件系统对象
     */
    @Before
    public void init() throws IOException, URISyntaxException, InterruptedException {
        //构造一个配置对象，将要设置的配置添加到该对象中
        Configuration conf = new Configuration();
        //如何去添加
        conf.set("fs.defaultFS", "hdfs://mini1:9000");
        //如果这样加载配置，需要将对应名字的文件放入resources目录中
        //而且结构必须与hadoop的配置文件的结构是一样的
        conf.addResource("cor-site.xml");
        //配置文件的优先级
        //1、conf.set(String name,String value)
        //2、将配置文件放入classpath下的
        //3、默认的配置文件
        conf.set("dfs.replication", "2");

        //权限解决第一种方式配置JVM的运行参数
        //-DHADOOP_USER_NAME=root
        //第二种方式设置临时环境变量
        // System.setProperty("HADOOP_USER_NAME","root");
        //fs = FileSystem.get(conf);
        //第三种方式，改变获取文件系统对象的方法
        fs = FileSystem.get(new URI("hdfs://mini1:9000"), conf, "root");
        //第四种方式关闭权限验证，重启集群
        /**
         *
         <!--是否开启hdfs的文件系统权限-->
         <property>
         <name>dfs.permissions.enabled</name>
         <value>false</value>
         </property>
         */
    }

    @Test
    public void testAddFileToHdfs() throws IOException {
        fs.copyFromLocalFile(new Path("E:\\Project\\UserPortrait\\sourceData\\out\\part.snappy.parquet"), new Path("/input/data.parquet"));
    }

    // hadoop  fs -rm -r /input/parquet

    @Test
    public void testDownLoadFileToLocal() throws IOException {
        fs.copyToLocalFile(
                new Path("/input/1.txt"),
                new Path("E:/MavenSparkProject/src/main/scala/com/qf/myscala/test/")
        );

    }

    @Test
    public void test() throws IOException {

       fs.mkdirs(new Path("/words"));
    }



}


