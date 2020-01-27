package com.lucho.spark;

import com.lucho.aws.S3Writer;
import com.lucho.domain.Log;
import com.lucho.utils.Utils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import scala.Tuple2;

import java.util.*;

public class SparkConsumer {

    private  Broadcast<List<String>> broadcastvar;
    private  JavaStreamingContext jssc;
    private  OffsetRange[] offsetRanges;
    private Collection<String> topics;
    private Map<String, Object> kafkaParams;
    private S3Writer s3Writer;

    public SparkConsumer(){
        System.setProperty("AWS_ACCESS_KEY_ID", "AKIA6CM2WR6UI46RMI5I");
        System.setProperty("AWS_SECRET_ACCESS_KEY", "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");
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
        jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        broadcastvar = jssc.sparkContext().broadcast(new ArrayList<String>());
        s3Writer = new S3Writer();
        kafkaParams = new HashMap<String, Object>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        s3Writer = new S3Writer();

        offsetRanges = new OffsetRange[]{
                // topic, partition, inclusive starting offset, exclusive ending offset
                OffsetRange.create("topic_streams", 0, 0, 100),
                OffsetRange.create("topic_streams", 1, 0, 100)
        };

        topics = Arrays.asList("topic_streams");



    }

    public void run() throws InterruptedException {

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );


        stream.foreachRDD(rdd -> {
            offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            // some time later, after outputs have completed
        });


        JavaPairDStream<String,String> pairDStream = stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));

        //        pairDStream.foreachRDD( rdd -> rdd.repartition(1).saveAsTextFile("file:///home/luciano/Documentos/kafka-to-s3"));
        // pairDStream.foreachRDD( rdd -> rdd.repartition(1).saveAsTextFile("s3a://AKIA6CM2WR6UI46RMI5I:rgEVZC+NtPo89mmdr392kZM1Lh+ncYSsiaZV4dgi@nuevo-bucket-para-kafka/kafka-logs/archivo.txt"));

        pairDStream.foreachRDD(
                rdd ->
                {
                    if(!rdd.isEmpty()){
                        JavaRDD<String> simplerdd = rdd.values();
                        JavaRDD<Log> logsRDD = simplerdd.map(s -> new Log(s));
                       //logsRDD.foreach( log ->
                       //        {
                       //            String jsonLog = Utils.jsonLog(log);
                       //            broadcastvar.value().add(jsonLog);
                       //        }
                       //);
                        logsRDD.foreach(new LogsFunction(broadcastvar));

                        System.out.println("imprimiendo: " + simplerdd.collect());
                        System.out.println("Este es el tamaño de la lista: " + broadcastvar.value().size());
                        if(broadcastvar.value().size() >= 20){
                            s3Writer.write(broadcastvar.value());
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


    public  void redefine(){
        broadcastvar.destroy();
        broadcastvar = jssc.sparkContext().broadcast(new ArrayList<String>());
    }

}
