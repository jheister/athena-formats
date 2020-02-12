package jheister;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClient;
import com.amazonaws.services.glue.model.CrawlerMetrics;
import com.amazonaws.services.glue.model.CrawlerTargets;
import com.amazonaws.services.glue.model.CreateCrawlerRequest;
import com.amazonaws.services.glue.model.CreateDatabaseRequest;
import com.amazonaws.services.glue.model.DatabaseInput;
import com.amazonaws.services.glue.model.DeleteBehavior;
import com.amazonaws.services.glue.model.GetCrawlerMetricsRequest;
import com.amazonaws.services.glue.model.S3Target;
import com.amazonaws.services.glue.model.SchemaChangePolicy;
import com.amazonaws.services.glue.model.StartCrawlerRequest;
import com.amazonaws.services.glue.model.UpdateBehavior;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDate;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

public class CreatePriceFeedInAthena {
    public static final Properties PROPERTIES = loadProps();
    public static final String CRAWLER_NAME = "test crawler";
    public static final String FEED_NAME = "eod_prices";
    public static final String DB_NAME = "test2";
    public static final String BUCKET_NAME = PROPERTIES.getProperty("bucket.name");
    public static final String CRAWLER_ROLE = PROPERTIES.getProperty("crawler.role");

    /*
    * todo: cleanup dependence on the role (or work out what it needs to be)
    * todo: decouple from orc file name (and use folder instead)
    * todo: make feed use multiple files to demonstrate temporal queries
    * todo: example queries:
    *    SELECT * FROM "test2"."input_data" where price_date > cast('2019-03-04' as DATE) and price_date < cast('2019-04-04' as DATE) limit 10;
    *    show scanned quantity querying by date vs. instrument
    * todo: how does it work with map in orc file
    * todo: test when different files have different schemas
    *
    *
    *
    *
    * Create user with credentials in .aws/credentials
    *    - S3 full
    *    - Athena full
    *    - AWSGlueServiceRole
    *    - AWSGlueConsoleFullAccess
    * Create bucket for athena
    * Create role for crawler
    *     AWSGlueServiceRole
    *     S3 read-only
    * */

    public static void main(String[] args) throws InterruptedException {
        AmazonS3 s3 = AmazonS3Client.builder().withRegion(Regions.EU_WEST_1).build();
        AWSGlue glue = AWSGlueClient.builder().withRegion(Regions.EU_WEST_1).build();

        System.out.println("Generating price file");
        generatePriceFile("month=2019-01", LocalDate.parse("2019-01-01"), LocalDate.parse("2019-02-01"), 100000);
        generatePriceFile("month=2019-02", LocalDate.parse("2019-02-01"), LocalDate.parse("2019-03-01"), 100000);
        generatePriceFile("month=2019-03", LocalDate.parse("2019-03-01"), LocalDate.parse("2019-04-01"), 100000);
        generatePriceFile("month=2019-04", LocalDate.parse("2019-04-01"), LocalDate.parse("2019-05-01"), 100000);
        generatePriceFile("month=2019-05", LocalDate.parse("2019-05-01"), LocalDate.parse("2019-06-01"), 100000);

        System.out.println("Uploading file");
        s3.putObject(BUCKET_NAME, FEED_NAME + "/month=2019-01/data.orc", new File("month=2019-01"));
        s3.putObject(BUCKET_NAME, FEED_NAME + "/month=2019-02/data.orc", new File("month=2019-02"));
        s3.putObject(BUCKET_NAME, FEED_NAME + "/month=2019-03/data.orc", new File("month=2019-03"));
        s3.putObject(BUCKET_NAME, FEED_NAME + "/month=2019-04/data.orc", new File("month=2019-04"));
        s3.putObject(BUCKET_NAME, FEED_NAME + "/month=2019-05/data.orc", new File("month=2019-05"));

        System.out.println("Creating athena database");
        glue.createDatabase(new CreateDatabaseRequest().withDatabaseInput(new DatabaseInput().withName(DB_NAME).withDescription("Example created from java")));

        System.out.println("Creating crawler");
        glue.createCrawler(new CreateCrawlerRequest()
                .withName(CRAWLER_NAME)
                .withDatabaseName(DB_NAME)
                .withRole(CRAWLER_ROLE)
                .withSchemaChangePolicy(new SchemaChangePolicy()
                        .withDeleteBehavior(DeleteBehavior.DELETE_FROM_DATABASE)
                        .withUpdateBehavior(UpdateBehavior.UPDATE_IN_DATABASE))
                .withTargets(new CrawlerTargets()
                        .withS3Targets(new S3Target().withPath("s3://" + BUCKET_NAME + "/" + FEED_NAME))));

        System.out.println("Running crawler");
        glue.startCrawler(new StartCrawlerRequest().withName(CRAWLER_NAME));

        while (true) {
            CrawlerMetrics result = glue.getCrawlerMetrics(new GetCrawlerMetricsRequest().withCrawlerNameList(CRAWLER_NAME)).getCrawlerMetricsList().get(0);
            int tablesCreated = result.getTablesCreated();
            if (tablesCreated > 0) {
                break;
            }
            System.out.println("Waiting for crawler to finish. Current state: " + result);
            Thread.sleep(5000);
        }
    }

    public static Properties loadProps() {
        try {
            Properties properties = new Properties();
            properties.load(new FileReader("aws.properties"));
            return properties;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public static void generatePriceFile(String filename, LocalDate start, LocalDate end, int instrumentCount) {
        try {
            OrcPriceFile orcPriceFile = new OrcPriceFile(filename);

            LocalDate date = start;

            List<UUID> ids = IntStream.range(0, instrumentCount).mapToObj(i -> UUID.randomUUID()).collect(toList());

            while (date.isBefore(end)) {
                Collections.shuffle(ids);
                for (UUID id : ids) {
                    orcPriceFile.write(id, date, Math.random());
                }
                date = date.plusDays(1);
            }

            orcPriceFile.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
