package test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

/**
 * Created by radovan on 4.12.17.
 */
public class BigPut {

    public static final int MAX_VALUE = 1_000_000_000;

    private static String tableName;

    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("test");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Configuration hbaseConf = HBaseConfiguration.create();
        tableName = "test_name";
        hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName);

        Connection connection = ConnectionFactory.createConnection(hbaseConf);
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            List<Put> puts = new ArrayList<>();
            for (long i = 0; i < MAX_VALUE; i++) {
                Put put = new Put(toBytes("TestRow" + i));
                put.addColumn(toBytes("a"), toBytes("test"), toBytes("some value " + i));
                puts.add(put);
            }
            table.put(puts);
        }
        connection.close();
        sc.stop();
    }
}
