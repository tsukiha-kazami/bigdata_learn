
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * 需求：开发kafka生产者代码
 */
public class KafkaProducerStudy {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //准备配置属性
        Properties props = new Properties();
        //kafka集群地址
        props.put("bootstrap.servers", "node01:9092,node02:9092,node03:9092");
        //acks它代表消息确认机制   // 1 0 -1 all
        /**
         * acks = 0: 表示produce请求立即返回，不需要等待leader的任何确认。
         *          这种方案有最高的吞吐率，但是不保证消息是否真的发送成功。
         *
         * acks = 1: 表示leader副本必须应答此produce请求并写入消息到本地日志，之后produce请求被认为成功. 如果leader挂掉有数据丢失的风险

         * acks = -1或者all: 表示分区leader必须等待消息被成功写入到所有的ISR副本(同步副本)中才认为produce请求成功。
         *   这种方案提供最高的消息持久性保证，但是理论上吞吐率也是最差的。
         */
        props.put("acks", "1");

        //重试的次数
        props.put("retries", 0);
        //缓冲区的大小  //默认32M
        props.put("buffer.memory", 33554432);
        //批处理数据的大小，每次写入多少数据到topic   //默认16KB
        props.put("batch.size", 16384);
        //可以延长多久发送数据   //默认为0 表示不等待 ，立即发送
        props.put("linger.ms", 1);
        //指定key和value的序列化器
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 100; i++) {
            //这里需要三个参数，第一个：topic的名称，第二个参数：表示消息的key,第三个参数：消息具体内容
            producer.send(new ProducerRecord<String, String>("test", Integer.toString(i), "hello-kafka-"+i));
        }
        producer.close();
    }

}