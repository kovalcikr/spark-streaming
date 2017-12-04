package test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * Created by radovan on 4.12.17.
 */
public class SparkJob {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("test");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set(TableInputFormat.INPUT_TABLE, "test_name");

        JavaPairRDD<ImmutableBytesWritable, Result> rdd = sc.newAPIHadoopRDD(hbaseConf, TableInputFormat.class,
                ImmutableBytesWritable.class, Result.class);

        JavaRDD<String> logData = rdd.map(tuple -> {
            System.out.println(Bytes.toString(tuple._1.get()));
            System.out.println(tuple._2);
            return Bytes.toString(tuple._2.getValue(Bytes.toBytes("a"), Bytes.toBytes("test")));
        });

        long numAs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) { return s.contains("a"); }
        }).count();

        long numBs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) { return s.contains("b"); }
        }).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);


//        org.apache.hadoop.conf.Configuration hconf = HBaseConfiguration.create();
//        JavaHBaseContext jhbc = new JavaHBaseContext(sc, hconf);
//        Scan scan1 = new Scan();
//        scan1.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, tableName.getBytes());
//
//// Create RDD
//        rdd = jhbc.hbaseRDD(tableName, scan1, new Function<Tuple2<ImmutableBytesWritable, Result>, Tuple2<ImmutableBytesWritable, Result>>() {
//            @Override
//            public Tuple2<ImmutableBytesWritable, Result> call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {
//                return immutableBytesWritableResultTuple2;
//            }
//        });
//
//        // Create streaming context and queue
//        JavaStreamingContext ssc = new JavaStreamingContext(sc);
//
//        Queue<JavaRDD<Tuple2<ImmutableBytesWritable, Result>>> queue = new Queue<JavaRDD<Tuple2<ImmutableBytesWritable, Result>>>();
//        queue.enqueue(rdd);
//
//        JavaDStream<Tuple2<ImmutableBytesWritable, Result>> ssc.queueStream(queue);
    }
}
