package com.lucho.main;

import java.util.*;
import com.lucho.aws.S3Writer;
import com.lucho.spark.SparkConsumer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.Tuple2;

public class Main
{
    public static Broadcast<List<String>> broadcastvar;
    public static JavaStreamingContext jssc;
    public static OffsetRange[] offsetRanges;


    public static void main(String[] args) throws InterruptedException {

        SparkConsumer sparkConsumer = new SparkConsumer();
        sparkConsumer.run();
    }
    public static void redefine(){
        broadcastvar.destroy();
        broadcastvar = jssc.sparkContext().broadcast(new ArrayList<String>());
    }
}
