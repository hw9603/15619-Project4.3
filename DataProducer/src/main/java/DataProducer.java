import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;

public class DataProducer {

    public static void main(String[] args) {
        /*
            Task 1:
            In Task 1, you need to read the content in the tracefile we give to you, 
            and create two streams, feed the messages in the tracefile to different 
            streams based on the value of "type" field in the JSON string.

            Please note that you're working on an ec2 instance, but the streams should
            be sent to your samza cluster. Make sure you can consume the topics on the
            master node of your samza cluster before make a submission. 

            Reference doc: https://kafka.apache.org/0100/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html

        */
        //TODO: Set Properties
        Properties props = new Properties();
        // Follow the example from https://kafka.apache.org/0100/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html
        props.put("bootstrap.servers", "ec2-18-206-242-91.compute-1.amazonaws.com:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Create the producer and configure the properties
        Producer<Integer, String> producer = new KafkaProducer<>(props);

        //TODO: Read json file and send to stream
        // Read the tracefile
        File file = new File("trace_task3");
        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line;
            // Scan the tracefile line by line
            while ((line = br.readLine()) != null) {
                // Parse every line and extract relevant information
                JSONObject jsonObject = new JSONObject(line);
                String type = jsonObject.getString("type");
                // Judge which topic to sent to based on the type
                if (type.equals("RIDER_INTEREST")) {
                    int userId = jsonObject.getInt("userId");
                    producer.send(new ProducerRecord<Integer, String>("events", userId % 5, userId, line));
                } else if (!type.equals("DRIVER_LOCATION")){
                    // Send to the events topic
                    int blockId = jsonObject.getInt("blockId");
                    producer.send(new ProducerRecord<Integer, String>("events", blockId % 5, blockId, line));
                }
            }
            // Finish the file reading process
            br.close();
        } catch (FileNotFoundException e) {
            System.err.println("tracefile not found!");
        } catch (IOException e) {
            System.err.println("IO exception!");
        }
        // Finish with the producer
        producer.close();
    }
}
