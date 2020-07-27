package mz.project.sparkboot.services;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;

@Service
public class WordService {

    @Autowired
    JavaSparkContext sc;

    @Autowired
    List<String> wasteList;

    public Map<String, Integer> getWordsMap(int number, String filename) {

        Set<String> wasteSet = new HashSet<>(wasteList);

        Broadcast<Set<String>> broadcastSet = sc.broadcast(wasteSet);

        JavaRDD<String> lines = sc.textFile("data/" + filename +".txt");

        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split("\\W+")).iterator())
                .map(String::toLowerCase)
                .filter(str -> str.length() > 1)
                .filter(str -> !broadcastSet.value().contains(str));

        Map<String, Integer> pairs = words
                .mapToPair(word -> new Tuple2<String, Integer>(word, 1))
                .reduceByKey(Integer::sum)
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .mapToPair(Tuple2::swap)
                .take(number)
                .stream()
                .collect(getCollector());
//                .collect(Collectors.toMap(tup -> tup._1, tup -> tup._2));

        return pairs;
    }

    private Collector<Tuple2<String, Integer>, Map<String, Integer>, Map<String, Integer>> getCollector() {
        return Collector.of(
                LinkedHashMap::new,
                (map, tup) -> map.put(tup._1, tup._2),
                (left, right) -> { left.putAll(right); return left;},
                Collector.Characteristics.IDENTITY_FINISH);
    }

}
