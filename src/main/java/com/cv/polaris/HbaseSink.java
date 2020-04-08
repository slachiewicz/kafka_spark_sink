package com.cv.polaris;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Timestamp;


public class HbaseSink extends ForeachWriter<Row> implements Serializable {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(JDBCSink.class);
    private static Logger log = Logger.getLogger(HbaseSink.class);

    private final String[] columnList;
    public org.apache.hadoop.hbase.client.Connection connection;
    String tableName = "ihub:some_table";
    String columnFamily = "cf0";
    private String rowKey;
    private Table table;
    //PropertiesConfig properties = new PropertiesConfig();

    public HbaseSink(String[] columnList) {
        this.columnList = columnList;
    }

    public boolean open(long partitionId, long epochId) {
        try {
            log.info("Hbase sink: In open method: " + new Timestamp(System.currentTimeMillis()));
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "sample_quorum");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            conf.set("zookeeper.znode.parent", "unsecure_some_stuff");
            conf.set("log4j.logger.org.apache.hadoop.hbase", "ERROR");
            connection = ConnectionFactory.createConnection(conf);
            LOGGER.info("Connection created:" + connection);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("Hbase sink: Error occurred in open method: " + new Timestamp(System.currentTimeMillis()), e);
        }
        return true;
    }

    public void process(Row value) {
        LOGGER.info("Start Processing Process...");
        try {
            log.info("Hbase sink: In process method: " + new Timestamp(System.currentTimeMillis()));
            LOGGER.info("Connection : " + connection);
            table = connection.getTable(TableName.valueOf(tableName));
            LOGGER.info("Table : " + table);
            rowKey = value.getAs("PAYMENT_EVENT_KEY").toString();
            LOGGER.info("Rowkey : " + rowKey);
            Put p = new Put(Bytes.toBytes(String.valueOf(rowKey)));
            for (int j = 0; j < columnList.length; j++) {
                if (value.get(j) == null) {
                    p.addColumn(Bytes.toBytes("cf0"),
                            Bytes.toBytes(columnList[j]), Bytes.toBytes(""));
                } else {
                    p.addColumn(Bytes.toBytes("cf0"),
                            Bytes.toBytes(columnList[j]), Bytes.toBytes(value.get(j).toString()));

                }
            }
            table.put(p);
        } catch (Exception e) {
            LOGGER.info("Exception :" + e);
            log.error("Hbase sink: Error occurred in process method: " + new Timestamp(System.currentTimeMillis()), e);
        }
    }

    public void close(Throwable errorOrNull) {
        try {
            log.info("Hbase sink: In close method: " + new Timestamp(System.currentTimeMillis()));
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Hbase sink: Error occurred in close method: " + new Timestamp(System.currentTimeMillis()), e);
        }
    }
}