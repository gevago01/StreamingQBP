package partitioning.methods;

import comparators.LongComparator;
import filtering.FilterEmptyAnswers;
import filtering.FilterNullQueries;
import filtering.ReduceNofTrajectories;
import key.selectors.CSVTrajIDSelector;
import key.selectors.TrajectoryTSSelector;
import map.functions.CSVRecToTrajME;
import map.functions.LineToCSVRec;
import map.functions.TrajToTSTrajs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.util.LongAccumulator;
import partitioners.IntegerPartitioner;
import projections.ProjectTimestamps;
import scala.Tuple2;
import trie.Query;
import trie.Trie;
import utilities.*;

import java.sql.Time;
import java.util.*;

/**
 * Created by giannis on 10/12/18.
 */
public class TimeSlicing {

    //for full dataset
//    private static final Long MIN_TIME = 1372636853000l;
//    private static final Long MAX_TIME = 1404172759000l;
    //for half dataset
//    private static final Long MIN_TIME = 1372636853000l;
//    private static final Long MAX_TIME = 1404172751000l;
    //for octant - 1/4
//    private static final Long MIN_TIME = 1372636951000l;
//    private static final Long MAX_TIME = 1404172591000l;
    //for onesix
    private static final Long MIN_TIME = 1372636951000l;
    private static final Long MAX_TIME = 1404172591000l;
    //for sample
//    private static final Long MIN_TIME = 1376904623000l;
//    private static final Long MAX_TIME = 1376916881000l;

    //    private static final Long MIN_TIME = 1372636951000l;
//    private static final Long MAX_TIME = 1404172591000l;

    public static List<Long> getTimeIntervals(long numberOfSlices, long minTime,long maxTime) {
        final Long timeInt = (maxTime - minTime) / numberOfSlices;

        List<Long> timePeriods = new ArrayList<>();
        for (long i = minTime; i < maxTime; i += timeInt) {
            timePeriods.add(i);
        }
        timePeriods.add(maxTime);


        return timePeriods;
    }

    public static void main(String[] args) {
        int numberOfSlices = Integer.parseInt(args[0]);
        String appName=TimeSlicing.class.getSimpleName()+numberOfSlices;
        String fileName = "file:///mnt/hgfs/VM_SHARED/samplePort.csv";
//        String fileName= "file:///mnt/hgfs/VM_SHARED/trajDatasets/octant.csv";
//        String fileName= "file:///mnt/hgfs/VM_SHARED/trajDatasets/half.csv";
//        String fileName= "file:///mnt/hgfs/VM_SHARED/trajDatasets/85TD.csv";
//        String fileName= "file:///mnt/hgfs/VM_SHARED/trajDatasets/onesix.csv";
//          String fileName= "file:////home/giannis/concatTrajectoryDataset.csv";
//        String fileName = "file:////data/half.csv";
//        String fileName= "hdfs:////half.csv";
//        String fileName = "hdfs:////concatTrajectoryDataset.csv";
//        String fileName= "hdfs:////onesix.csv";
//        String fileName= "hdfs:////octant.csv";

//        String fileName= "file:////home/giannis/octant.csv";
        SparkConf conf = new SparkConf().setAppName(appName).setMaster("local[*]").
                set("spark.executor.instances", "" + Parallelism.PARALLELISM);
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(5000));
        JavaInputDStream<ConsumerRecord<String, String>> queryStream = KafkaSetup.setup(ssc);

        JavaRDD<CSVRecord> records = sc.textFile(fileName).map(new LineToCSVRec());

        JavaRDD<Long> timestamps = records.map(new ProjectTimestamps());

        long min=timestamps.min(new LongComparator());
        long max=timestamps.max(new LongComparator());

        List<Long> timePeriods = getTimeIntervals(numberOfSlices,min,max);

        JavaPairDStream<Long, CSVRecord> queryRecords =
                queryStream.map(t -> new CSVRecord(t.value())).mapToPair(csvRec -> new Tuple2<>(csvRec.getTrajID(), csvRec));

        JavaPairDStream<Integer, Query> queries =
                queryRecords.groupByKeyAndWindow(new Duration(5000)).mapValues(new CSVRecToTrajME()).map(t -> new Query(t._2(), timePeriods,PartitioningMethods.TIME_SLICING)).mapToPair(q -> new Tuple2<>(q.getTimeSlice(), q));


        JavaPairRDD<Long, Trajectory> trajectoryDataset = records.groupBy(new CSVTrajIDSelector()).mapValues(new CSVRecToTrajME()).filter(new ReduceNofTrajectories()).flatMapValues(new TrajToTSTrajs(timePeriods));
        JavaPairRDD<Integer, Trie> trieDataset = trajectoryDataset. values().groupBy(new TrajectoryTSSelector()).flatMapValues(new Function<Iterable<Trajectory>, Iterable<Trie>>() {
            @Override
            public Iterable<Trie> call(Iterable<Trajectory> trajectories) throws Exception {
                List<Trie> trieList = new ArrayList<>();
                Trie trie = new Trie();
                for (Trajectory t : trajectories) {
                    trie.insertTrajectory2(t);

                    trie.timeSlice = t.timeSlice;
                }

                trieList.add(trie);
                return trieList;
            }
        });


        JavaPairRDD<Integer, Trie> partitionedTries = trieDataset.partitionBy(new IntegerPartitioner()).persist(StorageLevel.MEMORY_ONLY());

        partitionedTries.foreach(t -> System.out.println(t));
        Stats.nofTriesInPartitions(partitionedTries);

        JavaDStream<Set<Long>> answers = queries.transform(new Function<JavaPairRDD<Integer, Query>, JavaRDD<Set<Long>>>() {
            @Override
            public JavaRDD<Set<Long>> call(JavaPairRDD<Integer, Query> v1) throws Exception {
//                JavaPairRDD<Integer,Set<Long>> asd=v1.join(trieRDD).mapValues( t -> t._2().queryIndex(t._1()) );
                System.out.println("PERFORMING JOIN");
                System.out.println("queries:");
                v1.foreach(t -> System.out.println(t));
                return v1.join(partitionedTries).mapValues(t -> t._2().queryIndex(t._1())).values();

            }
        });


        answers.foreachRDD(new VoidFunction<JavaRDD<Set<Long>>>() {
            @Override
            public void call(JavaRDD<Set<Long>> setJavaRDD) throws Exception {
                System.out.println("NASWER::");
                System.out.println(setJavaRDD.collect());
            }
        });
        ssc.start();

        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }



    }
}
