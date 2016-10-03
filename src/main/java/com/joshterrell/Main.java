package com.joshterrell;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by josh on 10/3/16.
 */

public class Main {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("javatest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("some_text_file.txt");
        JavaRDD<String> words = lines.flatMap((line) -> {
            String[] lineWords = line.split(" ");
            return Arrays.asList(lineWords).iterator();
        });
        JavaPairRDD<String, Integer> ones = words.mapToPair((word) -> new Tuple2(word, 1));
        JavaPairRDD<String, Integer> wordCounts = ones.reduceByKey((count1, count2) -> count1 + count2);
        JavaPairRDD<String, Integer> sortedWordCounts = wordCounts
                .mapToPair((tup) -> tup.swap())
                .sortByKey(false)
                .mapToPair((tup) -> tup.swap());

        List<Tuple2<String, Integer>> sortedWordCountsLocal = sortedWordCounts.collect();
        sortedWordCountsLocal.forEach((tup) -> {
            System.out.printf("%2d: %s\n", tup._2(), tup._1());
        });
    }
}