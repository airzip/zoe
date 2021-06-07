package ioe.online.lake;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.time.Duration;
import java.util.Properties;

public class FlinkAtDataStream {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
        FlinkKafkaConsumer<String> data = new FlinkKafkaConsumer<>(
                "test",
                new SimpleStringSchema(),
                properties
        );
        data.assignTimestampsAndWatermarks(
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10))
        );

        //DataStream<String> dataStream = env.addSource(data);
    }
}
