package jheister;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClient;
import com.amazonaws.services.glue.model.DeleteCrawlerRequest;
import com.amazonaws.services.glue.model.DeleteDatabaseRequest;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

import static jheister.CreatePriceFeedInAthena.BUCKET_NAME;
import static jheister.CreatePriceFeedInAthena.FILE_NAME;

public class Cleanup {
    public static void main(String[] args) {
        AmazonS3 s3 = AmazonS3Client.builder().withRegion(Regions.EU_WEST_1).build();
        AWSGlue glue = AWSGlueClient.builder().withRegion(Regions.EU_WEST_1).build();

        try {
            glue.deleteCrawler(new DeleteCrawlerRequest().withName(CreatePriceFeedInAthena.CRAWLER_NAME));
        } catch (EntityNotFoundException e) {}

        try {
            glue.deleteDatabase(new DeleteDatabaseRequest().withName(CreatePriceFeedInAthena.DB_NAME));
        } catch (EntityNotFoundException e) {}

        s3.deleteObject(BUCKET_NAME, "input_data/" + FILE_NAME);
    }
}
