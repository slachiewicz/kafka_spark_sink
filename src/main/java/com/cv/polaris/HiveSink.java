package com.cv.polaris;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hive.streaming.HiveStreamingConnection;
import org.apache.hive.streaming.StreamingConnection;
import org.apache.hive.streaming.StreamingException;
import org.apache.hive.streaming.StrictDelimitedInputWriter;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.math.BigInteger;

public class HiveSink extends ForeachWriter<Row> implements Serializable {

    String metastore = "thrift://node1:9083,thrift://node2:9083";
    String dbName = "default";
    String tblName = "nrt_dummy";

    public StreamingConnection connection;

    public boolean open(long partitionId, long epochId) {
        // create delimited record writer whose schema exactly matches table schema
        StrictDelimitedInputWriter writer = StrictDelimitedInputWriter.newBuilder().withFieldDelimiter(',').build();
        // create and open streaming connection (default.src table has to exist already)
        StreamingConnection connection;
        try {
            connection = HiveStreamingConnection.newBuilder().withDatabase(dbName).withTable(tblName)
                    .withRecordWriter(writer)
                    // a .withHiveConf(hiveConf)
                    .connect();
        } catch (StreamingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        // System.out.println("Connection created:" + connection);
        return true;
    }

    //BigInteger.valueOf(value.getAs("payment_id"))
    public void process(Row value) {
        System.out.println("Start Processing Process...");
        try {
            System.out.println("Connection : " + connection);
            connection.beginTransaction();
            connection.write(Bytes.toBytes(value.getAs("EVENT_ID").toString() + ","
                    + BigInteger.valueOf(value.getAs("ID"))));
            Thread.sleep(55);
            connection.commitTransaction();
        } catch (Exception e) {
            System.out.println("Exception :" + e);
        }
    }

    public void close(Throwable errorOrNull) {
        try {
            if (connection != null)
                connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}