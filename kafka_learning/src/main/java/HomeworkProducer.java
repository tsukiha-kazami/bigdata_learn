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
public class HomeworkProducer {

  static final String TOPIC = "bigdata";

  public static void main(String[] args) throws InterruptedException {
    producer();
  }

  private static void producer() throws InterruptedException {
    System.out.println("启动 producer");
    final Producer<String, String> producer = new KafkaProducer<String, String>(conf());
    for (int i = 0; i < 20; i++) {
      final String key = "key:" + i;
      final int finalI = i;
      producer.send(new ProducerRecord<String, String>(TOPIC, key, "bigdata:" + i),
          new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
              if (e == null) {
                System.out.println("send "+key+" to " + recordMetadata.topic() + " success "+recordMetadata.partition());
              } else {
                e.printStackTrace();
                producer.send(new ProducerRecord<String, String>(TOPIC, key, "value:" + finalI));
              }
            }
          });
      Thread.sleep(100L);
    }
    producer.close();
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
