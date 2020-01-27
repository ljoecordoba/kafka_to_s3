package com.lucho.aws;




import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import java.util.List;
import java.util.Random;

public class S3Writer {
    AmazonS3  s3client;
    public S3Writer(){
        AWSCredentials credentials = new BasicAWSCredentials(
                "AKIA6CM2WR6UI46RMI5I",
                "rgEVZC+NtPo89mmdr392kZM1Lh+ncYSsiaZV4dgi"
        );
        s3client = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(Regions.US_WEST_2)
                .build();





    }

    public void write(List<String> list) {

        Random random =  new Random();
        String totalString = "";
        for (String s:
             list) {
            totalString = totalString + s + "\n";
        }
        s3client.putObject(
                "nuevo-bucket-para-kafka",
                "20191205/"+random.nextInt(1000),
                totalString
        );

    }

}

