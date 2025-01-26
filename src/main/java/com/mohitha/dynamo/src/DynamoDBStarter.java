package com.mohitha.dynamo.src;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DynamoDBStarter {
    public static void main(String[] args) {
        // Build the DynamoDB client
        DynamoDbClient dynamoDbClient = DynamoDbClient.builder()
                .region(Region.US_EAST_1) // Specify your region
                .credentialsProvider(DefaultCredentialsProvider.create()) // Use default AWS credentials
                .build();
        List<String> tableNames = listTable(dynamoDbClient);
        System.out.println("########### Describing one of the Table from Dynamodb(specific region): ");
        describeTable(dynamoDbClient, tableNames.get(0));

        // Writing an item to a dynamo db
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("user_id", AttributeValue.builder().s("1234").build()); // Replace with your primary key
        item.put("timestamp", AttributeValue.builder().n("1609459200").build());
        item.put("title", AttributeValue.builder().s("noway").build());

        System.out.println("########### Writing to one of the Table in Dynamodb(specific region): ");
        writeItem(dynamoDbClient, tableNames.get(0), item);
        System.out.println("########### Batch Write to one of the Table in Dynamodb(specific region): ");
        batchWriteItem(dynamoDbClient, tableNames.get(0));

        dynamoDbClient.close();
    }

    public static List<String> listTable(DynamoDbClient dynamoDbClient) {
        List<String> output = new ArrayList<>();
        try {
            // Create a request to list tables
            ListTablesRequest request = ListTablesRequest.builder().build();

            // Execute the request
            ListTablesResponse response = dynamoDbClient.listTables(request);

            // Print the list of table names
            System.out.println("Tables in dynamodb :");
            response.tableNames().forEach(x -> output.add(x));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return output;
    }

    public static void describeTable(DynamoDbClient dynamoDbClient, String tableName) {
        try {
            // Create DescribeTableRequest
            DescribeTableRequest request = DescribeTableRequest.builder()
                    .tableName(tableName)
                    .build();

            // Call DescribeTable
            DescribeTableResponse response = dynamoDbClient.describeTable(request);

            // Print table details
            System.out.println("Table Name: " + response.table().tableName());
            System.out.println("Table Status: " + response.table().tableStatus());
            System.out.println("Item Count: " + response.table().itemCount());
            System.out.println("Table Size (bytes): " + response.table().tableSizeBytes());
            System.out.println("Key Schema: " + response.table().keySchema());
            System.out.println("Provisioned Throughput: " + response.table().provisionedThroughput());
        } catch (DynamoDbException e) {
            System.err.println("Unable to describe table: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("Error formatting response as JSON: " + e.getMessage());
        }
    }

    public static void writeItem(DynamoDbClient dynamoDbClient, String tableName, Map<String, AttributeValue> item) {
        try {
            // Create a PutItemRequest
            PutItemRequest request = PutItemRequest.builder()
                    .tableName(tableName)
                    .item(item)
                    .build();

            // Execute the request
            PutItemResponse response = dynamoDbClient.putItem(request);
            System.out.println("Item successfully added to the table. Response: " + response);

        } catch (DynamoDbException e) {
            System.err.println("Failed to write item to table: " + e.getMessage());
        }
    }

    public static void batchWriteItem(DynamoDbClient dynamoDbClient, String tableName) {
        // preparing the items to be written
        List<WriteRequest> writeRequests = new ArrayList<>();
        Map<String,AttributeValue> itemMap = new HashMap<>();

        writeRequests.add(WriteRequest.builder()
                .putRequest(PutRequest.builder()
                        .item(createItem("987987", "NoCategory","Testing", "1609459300" )).build()).build());
        writeRequests.add(WriteRequest.builder()
                .putRequest(PutRequest.builder()
                        .item(createItem("4234253", "NullCategory","Testing2", "1609359300" )).build()).build());

        writeRequests.forEach(request -> System.out.println(request.putRequest().item()));

        // prepare the batch write item request
        BatchWriteItemRequest batchWriteItemRequest =
                BatchWriteItemRequest.builder().requestItems(java.util.Map.of(tableName, writeRequests)).build();
        try {
            BatchWriteItemResponse response = dynamoDbClient.batchWriteItem(batchWriteItemRequest);
            // Check for unprocessed items
            if (!response.unprocessedItems().isEmpty()) {
                System.out.println("Some items were not processed: " + response.unprocessedItems());
            } else {
                System.out.println("All items written successfully!");
            }
        } catch (DynamoDbException e) {
            System.err.println("Error performing batch write: " + e.getMessage());
        }
    }

    // Helper method to create an item
    private static java.util.Map<String, AttributeValue> createItem(
            String userId, String category, String title, String timestamp) {
        return Map.of(
                "user_id", AttributeValue.builder().s(userId).build(),
                "cat", AttributeValue.builder().s(category).build(),
                "title", AttributeValue.builder().s(title).build(),
                "timestamp", AttributeValue.builder().s(timestamp).build() // Sort Key (if applicable)
        );
    }
}
