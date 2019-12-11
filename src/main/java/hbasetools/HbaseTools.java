package hbasetools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @Description
 * @Author cqh <caoqingghai@1000phone.com>
 * @Version V1.0
 * @Since 1.0
 * @Date 2019/9/25 11：42
 */
public class HbaseTools {
    //log4j 打印信息
    private static final Logger logger = Logger.getLogger(HbaseTools.class);
    private static final String CONNECT_KEY="hbase.zookeeper.quorum";
    private static final String CONNECT_VALUE="mini1:2181,mini2:2181,mini3:2181";
    private static Connection conn = null;
    static {
        //1、获取连接配置对象
        Configuration conf = new Configuration();
        //2、设置hbase的连接属性
        conf.set(CONNECT_KEY,CONNECT_VALUE);
        //3、获取一个hbase的连接对象
        try {
            conn = ConnectionFactory.createConnection(conf);
            logger.info("获取连接成功");
        } catch (IOException e) {
            logger.error("获取hbase的连接异常",e);
        }
    }

    //获取一个hbase的管理对象
    public static Admin getAdmin(){
        Admin admin = null;
        try {
            admin=conn.getAdmin();
        } catch (IOException e) {
            logger.error("获取admin对象异常",e);
        }
        return admin;
    }

    //关闭admin对象
    public static void closeAdmin(Admin admin){
        if (admin!=null){
            try {
                admin.close();
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    //获取表的操作对象的方法
    public static Table getTable(String tableName){
        //HTable 线程安全的
        //Table 非线程安全的
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            logger.error("获取表的操作对象异常",e);
        }
        return table;
    }

    //关闭表的操作对象的方法
    public static void closeTable(Table table){
        if (table!=null){
            try {
                table.close();
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //打印一行数据的方法
    public static void printRs(Result result){
        try {
            System.out.print(new String(result.getRow()));
            CellScanner cellScanner = result.cellScanner();
            while(cellScanner.advance()){
                //获取当前的cell
                Cell current = cellScanner.current();
                System.out.print("\t"+new String(CellUtil.cloneFamily(current),"utf-8"));
                System.out.print(":"+new String(CellUtil.cloneQualifier(current),"utf-8"));
                System.out.print("\t"+new String(CellUtil.cloneValue(current),"utf-8"));
            }
            System.out.println("");
        } catch (IOException e) {
            logger.error("获取数据异常",e);
        }
    }
}