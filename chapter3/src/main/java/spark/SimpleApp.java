package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class SimpleApp {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("Spark WordCount Application (java)").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

//        String inputPath = "file:\\G:\\gitworkspace\\sparkResearch-master\\t.txt";
        String inputPath = "hdfs://hd-01:9000/user/a.txt";
        String outputPath = "hdfs://hd-01:9000/user/output/"+new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        JavaRDD<String> textFile = sc.textFile(inputPath);

        JavaPairRDD<String, Integer> counts = textFile
                //每一行都分割成单词，返回后组成一个大集合
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                //key是单词，value是1
                .mapToPair(word -> new Tuple2<>(word, 1))
                //基于key进行reduce，逻辑是将value累加
                .reduceByKey((a, b) -> a + b);

        //先将key和value倒过来，再按照key排序
        JavaPairRDD<Integer, String> sorts = counts
                //key和value颠倒，生成新的map
                .mapToPair(tuple2 -> new Tuple2<>(tuple2._2(), tuple2._1()))
                //按照key倒排序
                .sortByKey(false);

        //取前10个
        List<Tuple2<Integer, String>> top10 = sorts.take(10);

        //打印出来
        for(Tuple2<Integer, String> tuple2 : top10){
            System.out.println(tuple2._2() + "\t" + tuple2._1());
        }

        sc.parallelize(top10).coalesce(1).saveAsTextFile(outputPath);
//        javaRDD = javaRDD.filter(s->s.contains("a"));
//        long count = javaRDD.count();
    }
}
