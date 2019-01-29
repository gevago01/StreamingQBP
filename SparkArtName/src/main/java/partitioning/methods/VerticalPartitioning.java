package partitioning.methods;

import comparators.LongComparator;
import filtering.ReduceNofTrajectories2;
import key.selectors.CSVTrajIDSelector;
import map.functions.CSVRecToTrajME;
import map.functions.LineToCSVRec;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import partitioners.StartingRSPartitioner;
import projections.ProjectRoadSegments;
import scala.Tuple2;
import trie.Query;
import trie.Trie;
import utilities.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Created by giannis on 10/12/18.
 */
public class VerticalPartitioning {


    private static List<Long> sliceNofSegments2(JavaRDD<CSVRecord> records, int nofVerticalSlices) {
        JavaRDD<Long> roadSegments = records.map(new ProjectRoadSegments()).distinct().sortBy(new Function<Long, Long>() {
            @Override
            public Long call(Long v1) throws Exception {
                return v1;
            }
        }, true, 1);

        long min = roadSegments.min(new LongComparator());
        long max = roadSegments.max(new LongComparator());

        long timePeriod = max - min;

        long interval = timePeriod / nofVerticalSlices;

        List<Long> indices = new ArrayList<>();
        List<Long> roadIntervals = new ArrayList<>();
        for (long i = min; i < max; i += interval) {
            roadIntervals.add(i);
        }
        roadIntervals.remove(roadIntervals.size() - 1);
        roadIntervals.add(max);


        return roadIntervals;
    }

    private static List<Long> sliceNofSegments(JavaRDD<CSVRecord> records, int nofVerticalSlices) {
        JavaRDD<Long> roadSegments = records.map(new ProjectRoadSegments()).distinct().sortBy(new Function<Long, Long>() {
            @Override
            public Long call(Long v1) throws Exception {
                return v1;
            }
        }, true, 1);
        long nofSegments = roadSegments.count();
        JavaPairRDD<Long, Long> roadSegmentswIndex =
                roadSegments.zipWithIndex().mapToPair(new PairFunction<Tuple2<Long, Long>, Long, Long>() {
                    @Override
                    public Tuple2<Long, Long> call(Tuple2<Long, Long> t) throws Exception {
                        return new Tuple2<>(t._2(), t._1());
                    }
                });

        long interval = nofSegments / nofVerticalSlices;

        List<Long> indices = new ArrayList<>();
        for (long i = 0; i < nofSegments - 1; i += interval) {
            indices.add(i);
        }
        indices.add(nofSegments - 1);
        List<Long> roadIntervals = new ArrayList<>();
        for (Long l : indices) {
            roadIntervals.add(roadSegmentswIndex.lookup(l).get(0));
        }

        return roadIntervals;
    }


    public static void main(String[] args) {
        int nofVerticalSlices = Integer.parseInt(args[0]);
        String fileName = "file:///mnt/hgfs/VM_SHARED/samplePort.csv";
//        String fileName= "file:///mnt/hgfs/VM_SHARED/trajDatasets/85TD.csv";
//        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/concatTrajectoryDataset.csv";
//          String fileName= "file:////mnt/hgfs/VM_SHARED/trajDatasets/half.csv";
//        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/onesix.csv";
//          String fileName= "file:////mnt/hgfs/VM_SHARED/trajDatasets/octant.csv";
//        String fileName= "hdfs:////half.csv";
//        String fileName= "hdfs:////85TD.csv";
//        String fileName = "hdfs:////concatTrajectoryDataset.csv";
//        String fileName= "hdfs:////onesix.csv";
//        String fileName= "hdfs:////octant.csv";
//        String fileName= "file:////home/giannis/octant.csv";
        SparkConf conf = new SparkConf().setAppName(VerticalPartitioning.class.getSimpleName() + nofVerticalSlices)
                .setMaster("local[*]")
                .set("spark.executor.instances", "" + Parallelism.PARALLELISM);
//        SparkConf conf = new SparkConf().setAppName(VerticalPartitioning.class.getSimpleName()+nofVerticalSlices);

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(5000));
        JavaRDD<CSVRecord> records = sc.textFile(fileName).map(new LineToCSVRec());
        List<Long> roadIntervals = sliceNofSegments(records, nofVerticalSlices);


        JavaInputDStream<ConsumerRecord<String, String>> queryStream = KafkaSetup.setup(ssc);


        JavaPairDStream<Long, CSVRecord> queryRecords =
                queryStream.map(t -> new CSVRecord(t.value())).mapToPair(csvRec -> new Tuple2<>(csvRec.getTrajID(), csvRec));

        JavaPairDStream<Integer, Query> queries =
                queryRecords.groupByKeyAndWindow(new Duration(5000)).mapValues(new CSVRecToTrajME()).map(t -> new Query(t._2(), roadIntervals, PartitioningMethods.VERTICAL)).mapToPair(q -> new Tuple2<>(q.getVerticalID(), q));

//        stream.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String, String>>>() {
//            @Override
//            public void call(JavaRDD<ConsumerRecord<String, String>> consumerRecordJavaRDD) throws Exception {
//                consumerRecordJavaRDD.foreach(new VoidFunction<ConsumerRecord<String, String>>() {
//                    @Override
//                    public void call(ConsumerRecord<String, String> stringStringConsumerRecord) throws Exception {
//                        System.out.println(stringStringConsumerRecord);
//                    }
//                });
//            }
//        });


        JavaPairRDD<Integer, Trajectory> trajectoryDataset = records.groupBy(new CSVTrajIDSelector()).mapValues(new CSVRecToTrajME()).mapToPair(new PairFunction<Tuple2<Long, Trajectory>, Integer, Trajectory>() {
            @Override
            public Tuple2<Integer, Trajectory> call(Tuple2<Long, Trajectory> trajectoryTuple2) throws Exception {
                int x = new StartingRSPartitioner(roadIntervals).getPartition(trajectoryTuple2._2().getStartingRS());
                trajectoryTuple2._2().setVerticalID(x);
                return new Tuple2<Integer, Trajectory>(x, trajectoryTuple2._2());
            }
        }).filter(new ReduceNofTrajectories2());

        JavaPairRDD<Integer, Trie> trieRDD =
                trajectoryDataset.groupByKey().flatMap(new FlatMapFunction<Tuple2<Integer, Iterable<Trajectory>>, Trie>() {
                    @Override
                    public Iterator<Trie> call(Tuple2<Integer, Iterable<Trajectory>> stringIterableTuple2) throws Exception {
                        List<Trie> trieList = new ArrayList<>();
                        Iterable<Trajectory> trajectories = stringIterableTuple2._2();
                        Trie trie = new Trie();

                        for (Trajectory traj : trajectories) {
                            trie.insertTrajectory2(traj);
                            trie.setVerticalID(traj.getVerticalID());
                        }
                        trieList.add(trie);
                        return trieList.iterator();
                    }
                }).mapToPair(new PairFunction<Trie, Integer, Trie>() {
                    @Override
                    public Tuple2<Integer, Trie> call(Trie trie) throws Exception {
                        return new Tuple2<>(trie.getVerticalID(), trie);
                    }
                });


        JavaPairRDD<Integer, Trie> partitionedTries = trieRDD.partitionBy(new StartingRSPartitioner(roadIntervals)).persist(StorageLevel.MEMORY_ONLY());

        JavaDStream<Set<Long>> answers = queries.transform(new Function<JavaPairRDD<Integer, Query>, JavaRDD<Set<Long>>>() {
            @Override
            public JavaRDD<Set<Long>> call(JavaPairRDD<Integer, Query> v1) throws Exception {
//                JavaPairRDD<Integer,Set<Long>> asd=v1.join(trieRDD).mapValues( t -> t._2().queryIndex(t._1()) );
                System.out.println("PERFORMING JOIN");
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

//        Stats.nofTriesInPartitions(partitionedTries);
//        Stats.nofQueriesInEachVerticalPartition(partitionedQueries);
////        Stats.nofTriesInPartitions(partitionedTries);
////        System.out.println("nofTries::" + partitionedTries.values().collect().size());
//
    }


}