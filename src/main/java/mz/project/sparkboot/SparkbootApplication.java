package mz.project.sparkboot;

import org.apache.spark.SparkConf;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.context.annotation.Bean;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;


@SpringBootApplication
public class SparkbootApplication {

    @Value("${wastefile.name}")
    String wasteFile;

    public static void main(String[] args) {

        SpringApplication.run(SparkbootApplication.class, args);
    }

    @Bean
    public JavaSparkContext getContext() {
        SparkConf conf = new SparkConf().setAppName("taxiDrivers").setMaster("local[*]");
        return new JavaSparkContext(conf);
    }

    @Bean
    public List<String> wasteList() {
        File file = new File(wasteFile);
        String str = "";

        try (Scanner scan = new Scanner(file)) {
            str = scan.nextLine();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        return Arrays.asList(str.split(", "));
    }

}
