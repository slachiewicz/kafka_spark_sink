package com.cv.polaris;

import com.cv.polaris.*;
import com.databricks.spark.avro.SchemaConverters;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;

import java.io.*;

import org.apache.hadoop.fs.Path;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.streaming.Trigger.ProcessingTime;

//import com.nrt.test.sink.HiveSink;
//import com.hortonworks.hwc.HiveWarehouseSession;
//import com.nrt.test.sink.PhoenixSink;


public class PaymentEvent implements Serializable {
    private static Logger log = Logger.getLogger(PaymentEvent.class);

    public static void main(String[] args) throws Exception {
        log.info("Payment Event: Starting payment event processing: " + new Timestamp(System.currentTimeMillis()));

        SparkSession sparkSession = SparkSession.builder()
                .appName("PAYMENT_EVENT_APP_NRT")
                .enableHiveSupport()
                .getOrCreate();
        // Initializing Spark Conf
        log.info("Payment Event: Initializing spark conf: " + new Timestamp(System.currentTimeMillis()));


        Path path = new Path("Path to avro schema");
        FileSystem fs = null;
        Configuration conf = new Configuration();
        fs = FileSystem.get(path.toUri(), conf);
        FSDataInputStream inputStream = fs.open(path);
        BufferedReader buf = new BufferedReader(new InputStreamReader(inputStream));
        String jsonFormatSchema1 = org.apache.commons.io.IOUtils.toString(buf);
        Schema schema = new Schema.Parser().parse(jsonFormatSchema1);


        Dataset<Row> wtrandf = sparkSession
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "node1")
                .option("subscribe", "testTopic")
                .option("startingOffsets", "earliest")
                .load();



        Dataset<Row> finalDS = wtrandf.select(col("value"), col("offset"))
                .mapPartitions(new MapPartitionsFunction<Row, Row>() {
                    @Override
                    public Iterator<Row> call(Iterator<Row> it) throws IOException {

                        GenericRecord readRecord = null;
                        List<Row> rowlist = new ArrayList<Row>();
                        while (it.hasNext()) {
                            Row r = it.next();
                            readRecord = changeFromAvro2((byte[]) r.get(0), jsonFormatSchema1);
                            Long offset = (Long) r.get(1);

                            rowlist.add(rowForRecord(readRecord, offset));
                        }
                        return rowlist.iterator();
                    }
                }, org.apache.spark.sql.catalyst.encoders.RowEncoder
                        .apply(((StructType) SchemaConverters.toSqlType(schema).dataType()).add("offset", "Long")));



        //For HBaseSink, uncomment below and comment JDBCSink below
        //String[] columnList = finalDS.columns();
        //ForeachWriter writer = new HbaseSink(columnList);

        finalDS.printSchema();

        //For JDBCSink, comment HBaseSink above
        ForeachWriter writer = new JDBCSink();
        log.info("Payment Event: Writing final DS (union dataset): " + new Timestamp(System.currentTimeMillis()));

        //For Hive Sink, use below line
        //ForeachWriter writer = new HiveSink();


        StreamingQuery query = finalDS.writeStream()
                .foreach(writer)
                .outputMode(OutputMode.Update())
                .trigger(ProcessingTime("60 seconds"))
                .start();

        /*  Below is for Hive Sink  */
/*        StreamingQuery query = finalDS.writeStream()
                        .foreach(writer)
                        .format(HiveWarehouseSession.STREAM_TO_STREAM)
                        .option("database", "default")
                        .option("table", "nrt_dummy")
                        .option("metastoreUri","thrift://node1.com:9083,thrift://node2.com:9083")
                        .option("checkpointLocation", "hdfs://tmp/checkpoint")
                        .start();
*/

        query.awaitTermination();

    }

    public static GenericRecord changeFromAvro2(byte[] input, String jsonFormatSchema1) throws IOException {
        log.info("PaymentEventLookups: reading record with avro schema: " + new Timestamp(System.currentTimeMillis()));
        Schema schema1 = new Schema.Parser().parse(jsonFormatSchema1);
        Decoder decoder = DecoderFactory.get().binaryDecoder(input, null);
        DatumReader<GenericRecord> reader = new SpecificDatumReader<GenericRecord>(schema1);
        GenericRecord readRecord = null;

        try {
            readRecord = reader.read(null, decoder);
            System.out.println("record:  " + readRecord.get("id"));
            System.out.println("KAFKA record ID :: " + System.nanoTime() + " " + readRecord.get("id"));
        } catch (EOFException eofe) {
            eofe.printStackTrace();
        }
        return readRecord;
    }

    public static Row rowForRecord(GenericRecord record, Long offset) {
        List<Object> values = new ArrayList<>();
        log.info("PaymentEventLookups: get row values from record: " + new Timestamp(System.currentTimeMillis()));

        for (Schema.Field field : record.getSchema().getFields()) {
            Object value = record.get(field.name());

            Schema.Type fieldType = field.schema().getType();
            if (fieldType.equals(Schema.Type.UNION)) {
                fieldType = field.schema().getTypes().get(1).getType();
            }
            // Avro returns Utf8s for strings, which Spark SQL doesn't know how to use
            if (fieldType.equals(Schema.Type.STRING) && value != null) {
                value = value.toString();
            }

            values.add(value);
        }
        values.add(offset);
        return RowFactory.create(values.toArray());
    }



}


