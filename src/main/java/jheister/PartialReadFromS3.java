package jheister;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

import static jheister.CreatePriceFeedInAthena.BUCKET_NAME;
import static jheister.CreatePriceFeedInAthena.FEED_NAME;

public class PartialReadFromS3 {
    public static void main(String[] args) throws IOException {
        Path path = new Path(URI.create("s3a://" + BUCKET_NAME + "/" + FEED_NAME + "/month=2019-01/data.orc"));
        Configuration conf = new Configuration();
        conf.set("fs.s3a.aws.credentials.provider", DefaultAWSCredentialsProviderChain.class.getName());
        Reader reader = OrcFile.createReader(path, new OrcFile.ReaderOptions(conf));

        TypeDescription schema = reader.getSchema();
        System.out.println(schema);


        reader.getStripeStatistics().forEach(s -> {
            System.out.println("== Stripe ==");
            Arrays.stream(s.getColumnStatistics()).forEach(System.out::println);
        });
    }
}
