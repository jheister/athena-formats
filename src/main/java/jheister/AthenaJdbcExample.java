package jheister;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static com.simba.athena.amazonaws.regions.Regions.EU_WEST_1;

public class AthenaJdbcExample {
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        Connection connection = DriverManager.getConnection("jdbc:awsathena://AwsRegion=" + EU_WEST_1.getName() + ";AwsCredentialsProviderClass=com.simba.athena.amazonaws.auth.DefaultAWSCredentialsProviderChain;S3OutputLocation=s3://jheister-athena/output;");

        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM \"test2\".\"eod_prices\" limit 10");
        while (resultSet.next()) {
            System.out.println(resultSet.getString("price_date") + "\t" + resultSet.getString("id") + "\t" + resultSet.getDouble("close_price"));
        }

        resultSet.close();
        statement.close();
        connection.close();
    }
}
