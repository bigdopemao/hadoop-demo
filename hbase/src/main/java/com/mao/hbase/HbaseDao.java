package com.mao.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

/**
 * @author bigdope
 * @create 2018-08-14
 **/
public class HbaseDao {

    public static void main(String[] args) throws Exception {
        Configuration conf =  HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum", "testhbase");
        conf.set("hbase.zookeeper.quorum", "192.168.2.2");
//        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.4.1");

        System.out.println(conf.get("hbase.master"));

        HBaseAdmin admin = new HBaseAdmin(conf);

        TableName name = TableName.valueOf("nvshen");

        HTableDescriptor desc = new HTableDescriptor(name);

        HColumnDescriptor base_info = new HColumnDescriptor("base_info");
        HColumnDescriptor extra_info = new HColumnDescriptor("extra_info");
        base_info.setMaxVersions(5);

        desc.addFamily(base_info);
        desc.addFamily(extra_info);

        admin.createTable(desc);

    }

}
