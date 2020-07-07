/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: HbaseUtil
 * Author: yanglan88
 * Date: 2020/7/5 18:06
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 *
 * @author yanglan88
 * @create 2020/7/5
 * @since 1.0.0
 */


/**
 * @author yanglan88
 * @create 2020/7/5
 * @since 1.0.0
 */
package com.qf.bigdata.HbaseUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class HbaseUtil {
    private static LinkedList<Connection> pool = new LinkedList<>();

    static {
        try{

            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.rootdir", "hdfs://hadoop001:9000/hbase");
            conf.set("hbase.cluster.distributed", "true");
            conf.set("hbase.zookeeper.quorum", "hadoop001:2181,hadoop002:2181,hadoop003:2181");
            conf.set("hbase.regionserver.wal.codec", "org.apache.hadoop.hbase.regionserver.wal.IndexedWALEditCodec");
            for (int i = 0; i < 5; i++) {
                System.out.println("开始了");
                pool.push(ConnectionFactory.createConnection(conf));
                System.out.println("结束了");
            }

        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public static Connection getConnection(){
        while(pool.isEmpty()){

            System.out.println("Connection pool is null , please wait for a moment");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {

            }

        }
        return pool.poll();
    }

    //释放参数对象，将连接对象归还给连接池
    public static Map<Integer,Long> getColValue(Connection conn, TableName tableName, byte[] rowKey, byte[] Col){
        //声明Map存放最终结果
        Map<Integer,Long> partition2Offset = new HashMap();

        try{

            Table table = conn.getTable(tableName);
            Scan scan = new Scan();

            RowFilter rowFilter =
                    new RowFilter(CompareFilter.CompareOp.EQUAL,new BinaryComparator(rowKey));
            scan.setFilter(rowFilter);

            ResultScanner scanner = table.getScanner(scan);

            for (Result result : scanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    //col
                    byte[] column = CellUtil.cloneQualifier(cell);
                    //value
                    byte[] values = CellUtil.cloneValue(cell);

                    int partition = Integer.valueOf(new String(column));
                    long offset = Long.valueOf(new String(values));

                    partition2Offset.put(partition, offset);
                }
            }
            return partition2Offset;

        }catch(Exception e){
            e.printStackTrace();
        }
        return null;
    }

    // 释放连接对象，将连接对象归还给连接池
    public static void release(Connection connection) {
        pool.push(connection);
    }

    public static void set(Connection connection, TableName tableName, byte[] rowKey,  byte[] cf, byte[] partition, byte[] offset){
        try {

            Table table = connection.getTable(tableName);
            Put put = new Put(rowKey);
            put.addColumn(cf, partition, offset);
            table.put(put);
            table.close();

        }catch(Exception e){
            e.printStackTrace();
        }
    }
}

