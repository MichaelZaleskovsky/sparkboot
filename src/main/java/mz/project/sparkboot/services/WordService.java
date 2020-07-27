package mz.project.sparkboot.services;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collector;

@Service
public class WordService {

    @Autowired
    JavaSparkContext sc;

    @Autowired
    List<String> wasteList;

    public Map<String, Long> getWordsMap(int number, String filename) {

        Set<String> wasteSet = new HashSet<>(wasteList);

        Broadcast<Set<String>> broadcastSet = sc.broadcast(wasteSet);

        JavaRDD<String> lines = sc.textFile("data/" + filename +".txt");

        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split("\\W+")).iterator())
                .map(String::toLowerCase)
                .filter(str -> str.length() > 1)
                .filter(str -> !broadcastSet.value().contains(str))
                .persist(StorageLevel.MEMORY_AND_DISK());

        Map<String, Long> pairs = words
                .groupBy(w -> w)
                .mapToPair(tup -> new Tuple2<Long, String>(tup._2.spliterator().getExactSizeIfKnown(), tup._1))
                .sortByKey(false)
                .take(number)
                .stream()
                .collect(getCollector());

        return pairs;
    }

    private Collector<Tuple2<Long, String>, Map<String, Long>, Map<String, Long>> getCollector() {
        return Collector.of(
                LinkedHashMap::new,
                (map, tup) -> map.put(tup._2, tup._1),
                (left, right) -> { left.putAll(right); return left;},
                Collector.Characteristics.IDENTITY_FINISH);
    }

}
