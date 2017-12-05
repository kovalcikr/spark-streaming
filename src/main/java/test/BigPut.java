package test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

/**
 * Created by radovan on 4.12.17.
 */
public class BigPut {

    public static final int BATCH_COUNT = 10_000;
    public static final int BATCH_SIZE = 100_000;

    private static String tableNameString;

    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("test");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Configuration hbaseConf = HBaseConfiguration.create();
        tableNameString = "test_name";
        hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableNameString);

        TableName tableName = TableName.valueOf(tableNameString);

        Connection connection = ConnectionFactory.createConnection(hbaseConf);

        Admin admin = connection.getAdmin();

        System.out.println("Tables");
        Arrays.stream(admin.listTableNames()).map(TableName::getNameAsString).forEach(System.out::println);
        System.out.println();

        if (!admin.tableExists(tableName)) {
            System.out.println("creating table");
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            hTableDescriptor.addFamily(new HColumnDescriptor("a"));
            admin.createTable(hTableDescriptor);
        }

        try (Table table = connection.getTable(tableName)) {
            for (long i = 0; i < BATCH_COUNT; i++) {
                List<Put> puts = new ArrayList<>();
                for (long j = 0; j < BATCH_SIZE; j++) {
                    long num = i * j + j;
                    Put put = new Put(toBytes("TestRow" + num));
                    put.addColumn(toBytes("a"), toBytes("test"), toBytes("some value " + num));
                    puts.add(put);
                }
                long time = System.currentTimeMillis();
                table.put(puts);
                System.out.println("writed: " + ((double) i / BATCH_COUNT * 100) + " in " + (System.currentTimeMillis() - time));
            }
        }

        connection.close();
        sc.stop();
    }
}
