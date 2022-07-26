package ru.vtb.collection;
import com.siebel.data.SiebelPropertySet;
import com.siebel.eai.SiebelBusinessService;
import com.siebel.eai.SiebelBusinessServiceException;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.header.Header;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.List;
import java.util.Collections;
import java.util.stream.Collectors;
import java.time.Duration;

public class SiebelKafkaJBSAdapter extends SiebelBusinessService {
    @Override
    public void doInvokeMethod(String methodName, SiebelPropertySet psInput, SiebelPropertySet psOutput)
            throws SiebelBusinessServiceException {
        switch(methodName) {
            case "Send":
                sendMethod(psInput, psOutput);
                break;
            case "Receive":
                receiveMethod(psInput, psOutput);
                break;
            default:
                throw new SiebelBusinessServiceException("INVALID_METHOD", "Method '" + methodName +
                        "' is not supported");
        }
    }
    private void sendMethod(SiebelPropertySet psInput, SiebelPropertySet psOutput)
            throws SiebelBusinessServiceException {

            String configFilePath = psInput.getProperty("configFilePath");
            String topic = psInput.getProperty("topic");
            String key = psInput.getProperty("key");
            String value = psInput.getProperty("value");
            Enumeration<String> inputEnumeration = psInput.getPropertyNames();
            List<String> headers = Collections
                    .list(inputEnumeration)
                    .stream()
                    .filter(item -> item.startsWith("header_"))
                    .collect(Collectors.toList());
            if (configFilePath == null || topic == null || key == null || value == null) {
                throw new SiebelBusinessServiceException("NO_REQUIRED_INPUTS", "No required inputs");
            }
            Properties props;
            try {
                props = loadConfig(configFilePath);
            } catch (IOException e) {
                throw new SiebelBusinessServiceException("INVALID_CONFIG_FILE", e.getMessage());
            }
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            System.out.printf("producing record: %s\t%s\t%s%n", topic, key, value);

            Producer<String, String> producer = new KafkaProducer<String, String>(props);

            try {
                long time = System.currentTimeMillis();
                final ProducerRecord<String, String> record =
                    new ProducerRecord<>(topic, key, value);
                for (String header : headers) {
                    String headerValue = psInput.getProperty(header);
                    String headerName = header.replace("header_", "");
                    record.headers().add(headerName, headerValue.getBytes());
                    System.out.printf("adding header: %s\t%s\t%n", headerName, headerValue);
                }
                RecordMetadata metadata = producer.send(record).get();
                long elapsedTime = System.currentTimeMillis() - time;
                psOutput.setProperty("infoMessage", String.format("sent record(key=%s value=%s) " +
                    "meta(partition=%d, offset=%d) time=%d\n",
                    record.key(), record.value(), metadata.partition(), metadata.offset(), elapsedTime));
            } catch(Exception e) {
                throw new SiebelBusinessServiceException("SEND_ERROR", e.getMessage());
            } finally {
                producer.flush();
                producer.close();
            }
    }
    private void receiveMethod(SiebelPropertySet psInput, SiebelPropertySet psOutput)
            throws SiebelBusinessServiceException {

            String configFilePath = psInput.getProperty("configFilePath");
            String topic = psInput.getProperty("topic");
            if (configFilePath == null || topic == null) {
                throw new SiebelBusinessServiceException("NO_REQUIRED_INPUTS", "No required inputs");
            }
            Properties props;
            try {
                props = loadConfig(configFilePath);
            } catch (IOException e) {
                throw new SiebelBusinessServiceException("INVALID_CONFIG_FILE", e.getMessage());
            }
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            System.out.printf("receiving record from topic: %s%n", topic);

            Consumer<String, String> consumer = new KafkaConsumer<>(props);

            try {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    for (Header header : consumerRecord.headers()) {
                        System.out.println("header key " + header.key() + "header value " + new String(header.value()));
                    }
                }
            } catch(Exception e) {
                throw new SiebelBusinessServiceException("RECEIVE_ERROR", e.getMessage());
            } finally {
                consumer.close();
            }
            psOutput.setType("messageList");
            SiebelPropertySet message1 = new SiebelPropertySet();
            message1.setType("message");
            message1.setProperty("value", "receivedValue1");
            SiebelPropertySet message1header1 = new SiebelPropertySet();
            message1header1.setType("header");
            message1header1.setProperty("header_11", "header_11_value");
            message1.addChild(message1header1);
            SiebelPropertySet message1header2 = new SiebelPropertySet();
            message1header2.setType("header");
            message1header2.setProperty("header_12", "header_12_value");
            message1.addChild(message1header2);
            psOutput.addChild(message1);
            SiebelPropertySet message2 = new SiebelPropertySet();
            message2.setType("message");
            message2.setProperty("value", "receivedValue2");
            SiebelPropertySet message2header1 = new SiebelPropertySet();
            message2header1.setType("header");
            message2header1.setProperty("header_21", "header_21_value");
            message2.addChild(message2header1);
            SiebelPropertySet message2header2 = new SiebelPropertySet();
            message2header2.setType("header");
            message2header2.setProperty("header_22", "header_22_value");
            message2.addChild(message2header2);
            psOutput.addChild(message2);
    }
    public static Properties loadConfig(final String configFile) throws IOException {
        if (!Files.exists(Paths.get(configFile))) {
            throw new IOException(configFile + " not found.");
        }
        final Properties cfg = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            cfg.load(inputStream);
        }
        return cfg;
    }
}
