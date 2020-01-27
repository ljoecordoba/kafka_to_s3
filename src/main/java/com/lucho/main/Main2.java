package com.lucho.main;

import java.time.Duration;
import java.util.*;

import com.lucho.aws.S3Writer;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.Tuple2;

public class Main2
{
    public static Broadcast<List<String>> broadcastvar;
    public static JavaStreamingContext jssc;
    public static OffsetRange[] offsetRanges;

    public static void main(String[] args) throws InterruptedException {


        System.setProperty("AWS_ACCESS_KEY_ID", "AKIA6CM2WR6UI46RMI5I");
        System.setProperty("AWS_SECRET_ACCESS_KEY", "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");
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
        jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        broadcastvar = jssc.sparkContext().broadcast(new ArrayList<String>());
        S3Writer s3Writer = new S3Writer();




        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        Random random = new Random();

        Collection<String> topics = Arrays.asList("topic_streams");



        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );


        stream.foreachRDD(rdd -> {
            offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();

            // some time later, after outputs have completed
            ((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
        });


        JavaPairDStream<String,String> pairDStream = stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));

        //        pairDStream.foreachRDD( rdd -> rdd.repartition(1).saveAsTextFile("file:///home/luciano/Documentos/kafka-to-s3"));
        // pairDStream.foreachRDD( rdd -> rdd.repartition(1).saveAsTextFile("s3a://AKIA6CM2WR6UI46RMI5I:rgEVZC+NtPo89mmdr392kZM1Lh+ncYSsiaZV4dgi@nuevo-bucket-para-kafka/kafka-logs/archivo.txt"));


        pairDStream.foreachRDD(
                rdd ->
                {
                    if(!rdd.isEmpty()){
                        JavaRDD<String> simplerdd = rdd.values();
                        //                   rdd.saveAsTextFile("s3a://AKIA6CM2WR6UI46RMI5I:r,gEVZC+NtPo89mmdr392kZM1Lh+ncYSsiaZV4dgi@nuevo-bucket-para-kafka/kafka-logs/20191205/"+random.nextInt(1000));
                        simplerdd.foreach( l ->
                                {
                                    System.out.println("Agregando mensaje: " + l);
                                    broadcastvar.value().add(l);
                                }
                        );
                        System.out.println("imprimiendo: " + simplerdd.collect());
                        System.out.println("Este es el tamaño de la lista: " + broadcastvar.value().size());
                        if(broadcastvar.value().size() == 20){
                            //s3Writer.write(broadcastvar.value());
                            System.out.println("Esta es la lista: " + broadcastvar.value());
                            redefine();
                            ((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
                        }
                    }
                }
        );




        jssc.start();
        jssc.awaitTermination();
    }
    public static void redefine(){
        broadcastvar.destroy();
        broadcastvar = jssc.sparkContext().broadcast(new ArrayList<String>());
    }
}
