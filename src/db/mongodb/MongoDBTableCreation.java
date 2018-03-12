package db.mongodb;

import java.text.ParseException;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;

public class MongoDBTableCreation {
  public static void main(String[] args) throws ParseException {
    MongoClient mongoClient = new MongoClient();
    MongoDatabase db = mongoClient.getDatabase(MongoDBUtil.DB_NAME);

    db.getCollection("users").drop();
    db.getCollection("items").drop();

    // Insert a fake user
    db.getCollection("users")
        .insertOne(new Document().append("first_name", "John").append("last_name", "Smith")
            .append("password", "3229c1097c00d497a0fd282d586be050").append("user_id", "1111"));
    // make sure user_id is unique.
    IndexOptions indexOptions = new IndexOptions().unique(true);

    // In key-value pair, use 1 as value for ascending index , -1 for descending index
    // Different to MySQL, users table in MongoDB also has history info.
    db.getCollection("users").createIndex(new Document("user_id", 1), indexOptions);

    // make sure item_id is unique.
    // Different to MySQL, items table in MongoDB also has categories info.
    db.getCollection("items").createIndex(new Document("item_id", 1), indexOptions);

    mongoClient.close();
    System.out.println("Import is done successfully.");
  }
}
