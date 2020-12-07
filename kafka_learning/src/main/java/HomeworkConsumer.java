import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @author Shi Lei
 * @create 2020-11-30
 */
public class HomeworkConsumer {

  static final String TOPIC = "homework";

  public static void main(String[] args) throws InterruptedException {
    consumer();
  }


  private static void consumer() {
    System.out.println("启动 consumer");
    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(conf());
    consumer.subscribe(Arrays.asList(TOPIC));

    final int minBatchSize = 5;

    List<ConsumerRecord<String, String>> buffer = new ArrayList<ConsumerRecord<String, String>>();
    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(100);
      for (ConsumerRecord<String, String> record : records) {
        buffer.add(record);
      }
      if (buffer.size() >= minBatchSize) {
        System.out.println("缓冲区的数据条数：" + buffer.size());
        System.out.println("我已经处理完这一批数据了...");
        consumer.commitSync();
        buffer.clear();
      }
    }
  }


  public static Properties conf() {
    Properties props = new Properties();
    props.put("bootstrap.servers", "node01:9092,node02:9092,node03:9092");
    props.put("group.id", "homework");
    props.put("acks", "1");

    props.put("retries", 0);
    props.put("buffer.memory", 33554432);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);

    props.put("auto.offset.reset", "earliest");
    props.put("enable.auto.commit", "false");

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    return props;
  }


}
