package partitioning.methods;

import com.google.common.collect.Lists;
import filtering.ReduceNofTrajectories;
import key.selectors.CSVTrajIDSelector;
import map.functions.CSVRecordToTrajectory;
import map.functions.HCSVRecToTrajME;
import map.functions.LineToCSVRec;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import partitioners.IntegerPartitioner;
import scala.Tuple2;
import trie.Query;
import trie.Trie;
import utilities.*;

import java.util.*;

/**
 * Created by giannis on 27/12/18.
 */
public class HorizontalPartitioning {


    public static void main(String[] args) {
        int horizontalPartitionSize = Integer.parseInt(args[0]);

        String appName = HorizontalPartitioning.class.getSimpleName() + horizontalPartitionSize;
//        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/onesix.csv";
//        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/half.csv";
//        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/octant.csv";
        String fileName = "file:///mnt/hgfs/VM_SHARED/samplePort.csv";
//        String fileName= "hdfs:////concatTrajectoryDataset.csv";
//        String fileName = "hdfs:////85TD.csv";
//        String fileName = "hdfs:////half.csv";
//        String fileName = "hdfs:////onesix.csv";
//        String fileName = "hdfs:////octant.csv";
//        String fileName = "hdfs:////65PC.csv";
        SparkConf conf = new SparkConf().setAppName(appName)
                .setMaster("local[*]")
                .set("spark.executor.instances", "" + Parallelism.PARALLELISM)
                .set("spark.executor.cores", "" + Parallelism.EXECUTOR_CORES);
//        SparkConf conf = new SparkConf().setAppName(appName);


        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(5000));
        JavaRDD<CSVRecord> records = sc.textFile(fileName).map(new LineToCSVRec());

        JavaInputDStream<ConsumerRecord<String, String>> queryStream = KafkaSetup.setup(ssc);

        JavaPairDStream<Long, CSVRecord> queryRecords =
                queryStream.map(t -> new CSVRecord(t.value())).mapToPair(csvRec -> new Tuple2<>(csvRec.getTrajID(), csvRec));
        JavaPairDStream<Integer, Query> queries =
                queryRecords.groupByKeyAndWindow(new Duration(5000)).mapValues(new CSVRecordToTrajectory()).flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Trajectory>, Integer, Query>() {
                    @Override
                    public Iterator<Tuple2<Integer, Query>> call(Tuple2<Long, Trajectory> trajectoryTuple2) throws Exception {
                        List<List<Long>> roadSegments = Lists.partition(trajectoryTuple2._2().roadSegments, horizontalPartitionSize);
                        List<List<Long>> timestamps = Lists.partition(trajectoryTuple2._2().timestamps, horizontalPartitionSize);

                        List<Tuple2<Integer, Query>> queryList = new ArrayList<>();

                        int horizontalID = 0;
                        for (int i = 0; i < roadSegments.size(); i++) {
                            List<Long> subSegments = roadSegments.get(i);
                            List<Long> subTimestamps = timestamps.get(i);

                            Query query = new Query(subTimestamps.get(0), subTimestamps.get(subTimestamps.size() - 1), subSegments);
                            query.setQueryID(trajectoryTuple2._2().getTrajectoryID());
                            query.setHorizontalPartition(horizontalID++);
                            queryList.add(new Tuple2<>(query.getHorizontalPartition(), query));

                        }
                        return queryList.iterator();
                    }
                });


        JavaPairRDD<Long, Iterable<CSVRecord>> recordsCached = records.groupBy(new CSVTrajIDSelector()).cache();
        JavaPairRDD<Integer, Trajectory> trajectoryDataset = recordsCached
                .filter(new Function<Tuple2<Long, Iterable<CSVRecord>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<Long, Iterable<CSVRecord>> v1) throws Exception {
                        return v1._2().spliterator().getExactSizeIfKnown() > ReduceNofTrajectories.MAX_TRAJECTORY_SIZE ? false : true;
                    }
                })
                .flatMapValues(new HCSVRecToTrajME(horizontalPartitionSize)).
                        mapToPair(new PairFunction<Tuple2<Long, Trajectory>, Integer, Trajectory>() {
                            @Override
                            public Tuple2<Integer, Trajectory> call(Tuple2<Long, Trajectory> trajectoryTuple2) throws Exception {
                                Integer horizontalID = trajectoryTuple2._2().getHorizontalID();
                                return new Tuple2<>(horizontalID, trajectoryTuple2._2());
                            }
                        });


        JavaPairRDD<Integer, Trie> trieRDD = trajectoryDataset.groupByKey().flatMap(new FlatMapFunction<Tuple2<Integer, Iterable<Trajectory>>, Trie>() {
            @Override
            public Iterator<Trie> call(Tuple2<Integer, Iterable<Trajectory>> integerIterableTuple2) throws Exception {
                List<Trie> trieList = new ArrayList<>();
                Iterable<Trajectory> trajectories = integerIterableTuple2._2();
                int horizontalID = integerIterableTuple2._1();
                Trie trie = new Trie();
                trie.setHorizontalTrieID(horizontalID);


                for (Trajectory traj : trajectories) {
                    trie.insertTrajectory2(traj);
                }
                trieList.add(trie);
                return trieList.iterator();
            }
        }).mapToPair(new PairFunction<Trie, Integer, Trie>() {
            @Override
            public Tuple2<Integer, Trie> call(Trie trie) throws Exception {
                return new Tuple2<>(trie.getHorizontalTrieID(), trie);
            }
        });


        JavaPairRDD<Integer, Trie> partitionedTries = trieRDD.partitionBy(new IntegerPartitioner()).persist(StorageLevel.MEMORY_ONLY());


        JavaPairDStream<Long,Set<Long>> answers =
                queries.transform(new Function<JavaPairRDD<Integer, Query>, JavaRDD<Tuple2<Long,Set<Long>>>> () {
            @Override
            public JavaRDD<Tuple2<Long,Set<Long>>> call(JavaPairRDD<Integer, Query> v1) throws Exception {
//                JavaPairRDD<Integer,Set<Long>> asd=v1.join(trieRDD).mapValues( t -> t._2().queryIndex(t._1()) );
                System.out.println("PERFORMING JOIN");
                return v1.join(partitionedTries).mapToPair(new PairFunction<Tuple2<Integer,Tuple2<Query,Trie>>, Long, Set<Long>>() {
                    @Override
                    public Tuple2<Long, Set<Long>> call(Tuple2<Integer, Tuple2<Query, Trie>> t) throws Exception {
                        Set<Long> rs=t._2()._2().queryIndex(t._2()._1());
                        return new Tuple2<>(t._2()._1().getQueryID(),rs);
                    }
                }).map(t -> new Tuple2<>(t._1(),t._2()));

            }
        }).mapToPair(t -> new Tuple2<>(t._1(),t._2())).groupByKey().
                        mapValues(setsOfLongs -> { Set<Long> ans=new TreeSet<>(); setsOfLongs.forEach(longs ->  longs.forEach(e -> ans.add(e))); return ans;});


        answers.foreachRDD(new VoidFunction<JavaPairRDD<Long, Set<Long>>>() {
            @Override
            public void call(JavaPairRDD<Long, Set<Long>> longSetJavaPairRDD) throws Exception {
                longSetJavaPairRDD.foreach(new VoidFunction<Tuple2<Long, Set<Long>>>() {
                    @Override
                    public void call(Tuple2<Long, Set<Long>> longSetTuple2) throws Exception {
                        System.out.println(longSetTuple2);
                    }
                });
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
