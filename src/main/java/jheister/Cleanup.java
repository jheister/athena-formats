package jheister;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClient;
import com.amazonaws.services.glue.model.DeleteCrawlerRequest;
import com.amazonaws.services.glue.model.DeleteDatabaseRequest;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;

import java.util.List;
import java.util.stream.Collectors;

import static jheister.CreatePriceFeedInAthena.BUCKET_NAME;
import static jheister.CreatePriceFeedInAthena.FEED_NAME;

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


        while (true) {
            ObjectListing listing = s3.listObjects(BUCKET_NAME, FEED_NAME);
            if (listing.getObjectSummaries().isEmpty()) {
                break;
            }
            List<DeleteObjectsRequest.KeyVersion> keys = listing.getObjectSummaries().stream().map(s -> new DeleteObjectsRequest.KeyVersion(s.getKey())).collect(Collectors.toList());
            s3.deleteObjects(new DeleteObjectsRequest(BUCKET_NAME).withKeys(keys));
        }
    }
}
