package v1;


import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by 張燿峰
 * sparkStreaming消费kafka消息,实现wordCount
 *
 * @author 孤
 * @date 2019/4/28
 * @Varsion 1.0
 */
@Slf4j
public class KafkaStreaming {

    /**
     * 数据分割的规则
     */
    private static final Pattern SPACE = Pattern.compile(" ");

    /**
     * zookeeper主机
     */
    private static final String HOST = "192.168.91.151:2181";

    /**
     * 分组ID
     */
    private static final String GROP = "test-consumer-group";

    /**
     * 主题ID
     */
    private static final String TOPIC = "test";

    /**
     * 分片
     */
    private static final Integer THREAD = 1;

    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME","hadoop");
        SparkConf sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]");
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(60));
        //设置检查点
        streamingContext.checkpoint("hdfs://192.168.91.150:9000/checkpoint");
        Map<String, Integer> topicThread = new HashMap<>(1);
        topicThread.put(TOPIC, THREAD);
        JavaPairInputDStream<String, String> dStream = KafkaUtils.createStream(streamingContext, HOST, GROP, topicThread);

        JavaDStream<String> words = dStream.flatMap((FlatMapFunction<Tuple2<String, String>, String>) stringStringTuple2 -> Arrays.asList(SPACE.split(stringStringTuple2._2)).iterator());

        //统计
        JavaPairDStream<String, Integer> result = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                System.out.println("recevie:"+s);
                return new Tuple2<>(s,1);
            }
        }).reduceByKey((Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2);

        try {
            result.print();
            streamingContext.start();
            streamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
