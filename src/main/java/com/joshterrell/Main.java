package com.joshterrell;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import scala.Tuple2;

import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by josh on 10/3/16.
 */

public class Main {
    protected JavaSparkContext sc;

    public Main(JavaSparkContext sc) {
        this.sc = sc;
    }

    public static void main(String[] args) {
        Namespace ns = parseArgs(args);
        String path = ns.getString("file");
        int limit = ns.getInt("limit");

        ApplicationContext context = new ClassPathXmlApplicationContext("Beans.xml");
        Main main = (Main) context.getBean("main");

        main.run(path, limit);
    }

    public static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("count words")
                .defaultHelp(true)
                .description("count the unique words in a text file");
        parser.addArgument("file")
                .type(String.class)
                .help("path to a text file");
        parser.addArgument("--limit")
                .type(Integer.class)
                .setDefault(10)
                .help("how many results to limit to?");

        return parser.parseArgsOrFail(args);
    }

    public static SparkConf createConf(String master, String appName) {
        return new SparkConf()
                .setMaster(master)
                .setAppName(appName);
    }

    public void run(String textFilePath, int limit) {
        JavaRDD<String> lines = sc.textFile(textFilePath);
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
        List<Tuple2<String, Integer>> sortedWordCountsLocal = sortedWordCounts.take(limit);

        int max_val = sortedWordCountsLocal.get(0)._2();
        int max_digits = (int)Math.floor(Math.log10(max_val)) + 1;
        String fmt = "%" + max_digits + "d: %s\n";
        sortedWordCountsLocal.forEach((tup) -> {
            System.out.printf(fmt, tup._2(), tup._1());
        });
    }
}