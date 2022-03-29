package examples.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

import org.bson.Document;

public class MongoDBDemo {
	public static void main(String[] args) {
		try {
			System.out.println("connecting...");
			MongoClient mongoClient = new MongoClient("localhost", 27017);
			MongoDatabase mongoDatabase = mongoClient.getDatabase("exp1");
			System.out.println("Successfully connected");
			// insert document
			MongoCollection<Document> collection = mongoDatabase.getCollection("Student");
			Document document = new Document("name", "Scofield").append("score",
					new Document("English", 45).append("Math", 89).append("Computer", 100));
			collection.insertOne(document);
			System.out.println("Successfully inserted");
			// query 'Scofield' info
			MongoCursor<Document> cursor = collection.find(new Document("name", "Scofield"))
								.projection(new Document("score", 1).append("_id", 0)).iterator();
			while (cursor.hasNext())
				System.out.println(cursor.next().toJson());
			mongoClient.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
