package pair;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;


/**
 * Created by 張燿峰
 * pairRDD入门案例
 *
 * @author 孤
 * @date 2019/3/19
 * @Varsion 1.0
 */
public class PairRdd {

    public static void run(JavaSparkContext sparkContext) {
        JavaRDD<String> rdd = sparkContext.parallelize(Arrays.asList("test", "java", "python"));


        PairFunction<String, String, String> pairFunction = new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2<>(s.split(" ")[0], s);
            }
        };


        //此处创建好pairRDD
        JavaPairRDD<String, String> pairRdd = rdd.mapToPair(pairFunction);

        //下层都是对pairRDD的操作演示

        /*合并含有相同键的值*/
        pairRdd.reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String v1, String v2) throws Exception {
                return v1 + v2;
            }
        });

        /*相同key的元素进行分组*/
        pairRdd.groupByKey();

        /*对pair中的每个值进行应用*/
        pairRdd.mapValues(new Function<String, Object>() {
            @Override
            public Object call(String v1) throws Exception {
                return v1 + "sirZ";
            }
        });

        /*返回只包含键的RDD*/
        pairRdd.keys();

        /*返回只包含值的RDD*/
        pairRdd.values();

        /*返回根据键排序的RDD*/
        pairRdd.sortByKey();

    }

    /*针对多个pairRDD元素的操作*/
    public static void runPair(JavaSparkContext sparkContext) {

        JavaRDD<String> rdd = sparkContext.parallelize(Arrays.asList("test", "java", "python"));
        JavaRDD<String> otherRDD = sparkContext.parallelize(Arrays.asList("golang", "php", "hadoop"));

        PairFunction<String, String, String> pairFunction = new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) {
                return new Tuple2<>(s.split(" ")[0], s);
            }
        };
        JavaPairRDD<String, String> pairRDD = rdd.mapToPair(pairFunction);
        JavaPairRDD<String, String> pairRDDOther = otherRDD.mapToPair(pairFunction);

        //创建好两个PairRDD之后开始操作

        //删除 ==pairRDD== 中键与pairRDDOther相同的元素
        JavaPairRDD<String, String> subRDD = pairRDD.subtractByKey(pairRDDOther);

        //内连接 inner join 查询
        JavaPairRDD<String, Tuple2<String, String>> jsonRDD = pairRDD.join(pairRDDOther);

        //右连接 right join 查询   //TODO 此处我理解是可以为null的二元组
        JavaPairRDD<String, Tuple2<Optional<String>, String>> rightRDD = pairRDD.rightOuterJoin(pairRDDOther);

        //左连接 left join 查询
        JavaPairRDD<String, Tuple2<String, Optional<String>>> leftRDD = pairRDD.leftOuterJoin(pairRDDOther);

        //将两个RDD中有相同键的数据分组  //TODO 此处我理解是迭代器
        JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> groupRDD = pairRDD.cogroup(pairRDDOther);


        //pairRDD也可以使用RDD的函数
        //筛选length小于20的元素
        Function<Tuple2<String, String>, Boolean> filterRDD = new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> v1) {
                return (v1._2.length() < 20);
            }
        };
        JavaPairRDD<String, String> filter = pairRDD.filter(filterRDD);

        JavaRDD<String> wordCount = sparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "5"));

        //返回一个可以迭代的集合
        JavaRDD<String> c = wordCount.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String v1) throws Exception {
                return Arrays.asList(v1.split(" ")).iterator();
            }
        });

        //现在的数据是 1,2,3,4,5
        JavaPairRDD<String, Integer> result = c.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
                //此时的数据是 {1,1},{2,1},{3,1},{4,1}...
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        /*还可以通过countByValue快速实现单词计数*/
        c.countByValue();

        JavaPairRDD<String, Integer> javaPairRDD = sparkContext.parallelizePairs(Arrays.asList(new Tuple2<>("alert", 10)));
        javaPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return null;
            }
        }, 1);
    }
}
