package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;

public class SimpleApp {
  public static void main(String[] args) throws IOException {
    SparkConf sparkConf = new SparkConf().setAppName("Simple Application").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);

    Job job = Job.getInstance();
    job.setInputFormatClass(CustomTextInputFormat.class);
    String logFile = "README.md";

    JavaPairRDD<LongWritable, Text> rdd = sc.newAPIHadoopFile(logFile, CustomTextInputFormat.class, LongWritable.class, Text.class, job.getConfiguration());
    System.out.println(rdd.map(tuple -> new Tuple2<>(tuple._1.get(), tuple._2.toString())).collect());
    String outputPath = "output_" + System.currentTimeMillis();
    rdd.saveAsNewAPIHadoopFile(outputPath, LongWritable.class, Text.class, CustomFileOutputFormat.class, job.getConfiguration());
    System.out.println("Output written to output directory");
    sc.stop();
  }
}