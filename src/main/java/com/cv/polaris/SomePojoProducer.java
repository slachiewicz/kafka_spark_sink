package com.cv.polaris;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;


public class SomePojoProducer {

    public static void main(String[] args) throws InterruptedException, IOException {


        String topic = args[0];
        int counter = 0;
        while (true) {
            TimeZone tz = TimeZone.getTimeZone("UTC");
            DateFormat df = new SimpleDateFormat(
                    "yyyy-MM-dd'T'HH24:mm.sss"); // Quoted "Z" to indicate UTC, no timezone offset
            df.setTimeZone(tz);
            ++counter;
            String lastdt = "";
            System.out.println("message count: " + counter);
            // read lookup file file
            try (BufferedReader bufferedReader = new BufferedReader(new FileReader("lookup"))) {
                lastdt = bufferedReader.readLine();
                System.out.println("bufferedReader: " + lastdt);
            } catch (FileNotFoundException e) {
                System.out.println("File Not Found Exception...");
            } catch (IOException e) {
            }
            System.out.println("from file " + lastdt);

            //Kafka producer
            Properties properties = producerProps("kafka.bootstrap.servers.sample");
            KafkaProducer<String, byte[]> messageProducer = new KafkaProducer<String, byte[]>(properties);
            Schema schema = new Schema.Parser().parse(new File("path_to_schema"));
            DatumWriter<GenericRecord> writer = new SpecificDatumWriter<GenericRecord>(schema);
            for (int i=0; i< 10; i++) {
                GenericRecord e1 = new GenericData.Record(schema);
                e1.put("id", "id" + 1);
                e1.put("name", "name"+ i);

                byte[] serializedBytes = getAvroBinaryData(schema, e1);
                System.out.println("Sending message in Time : " + lastdt.toString().substring(0, 19));
                //Sending data to kafka topic
                ProducerRecord<String, byte[]> message = new ProducerRecord<String, byte[]>(topic, serializedBytes);
                messageProducer.send(message);
                // de-serialize byte array to record

            }
            messageProducer.flush();
            messageProducer.close();

            //write timestamp in lookup file
            try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("lookup"))) {
                bufferedWriter.write(finalTimestamp(lastdt).toString());
            } catch (IOException e) {
            }
            TimeUnit.SECONDS.sleep(50);
            //      break;
        }   //closing of infinite loop
    }// closing of main

    public static Timestamp finalTimestamp(String lastdt) {
        // 2020-01-23 15:18:08.4
        int window = 60;
        String newUpdatedTime = lastdt.toString().substring(0, 19);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Date date1 = null;
        try {
            date1 = dateFormat.parse(newUpdatedTime);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Timestamp dateTimeStamp = new Timestamp(date1.getTime());
        //System.out.println("***********"+dateTimeStamp);
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(dateTimeStamp.getTime());
        cal.add(Calendar.SECOND, window);
        //cal.add(Calendar.MINUTE, window);
        Timestamp later = new Timestamp(cal.getTime().getTime());
        System.out.println("original timestamp  ============= " + dateTimeStamp);
        System.out.println("After addition =============" + later);
        return later;
    }

    public static String NotNull(String col) {
        if (col == null) {
            col = "";
        }
        return col;
    }

    private static Properties producerProps(String bootstrapServer) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.ByteArraySerializer.class.getName());
        props.put("client.id", "sample_clientId");
        return props;
    }

    private static GenericRecord readRecord(Schema schema, byte[] originalAvrodata) throws IOException {
        Decoder decoder = DecoderFactory.get().binaryDecoder(originalAvrodata, null);
        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
        GenericRecord readRecord = null;
        try {
            readRecord = reader.read(null, decoder);
        } catch (EOFException eofe) {
            eofe.printStackTrace();
        }
        return readRecord;
    }

    // takes the record to be serialized as an additonal parameter
    private static byte[] getAvroBinaryData(Schema schema, GenericRecord record) throws IOException {
        DatumWriter<GenericRecord> writer = new SpecificDatumWriter<GenericRecord>(schema);
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        Encoder e = EncoderFactory.get().binaryEncoder(os, null);
        writer.write(record, e);
        e.flush();
        byte[] byteData = os.toByteArray();
        return byteData;
    }


}
