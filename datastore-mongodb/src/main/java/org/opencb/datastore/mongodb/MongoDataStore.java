package org.opencb.datastore.mongodb;

import java.util.*;
import java.util.function.Consumer;

import com.mongodb.*;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by imedina on 22/03/14.
 *
 * This class models and configure a physical connection to a specific database of a MongoDB server, notice this is
 * different from the Java driver where all databases from a single MongoDB server share the same configuration.
 * Therefore, different configurations can be applied to different databases.
 *
 * @author imedina
 * @author cyenyxe
 */

public class MongoDataStore {

    private Map<String, MongoDBCollection> mongoDBCollections = new HashMap<>();

    private MongoClient mongoClient;
    private MongoDatabase db;
    private MongoDBConfiguration mongoDBConfiguration;

    protected Logger logger = LoggerFactory.getLogger(MongoDataStore.class);

    MongoDataStore(MongoClient mongoClient, MongoDatabase db, MongoDBConfiguration mongoDBConfiguration) {
        this.mongoClient = mongoClient;
        this.db = db;
        this.mongoDBConfiguration = mongoDBConfiguration;
    }

    public boolean test() {
        Document commandResult = db.runCommand(new BsonDocument("dbStats", new BsonInt32(1)));
        return commandResult != null && commandResult.getDouble("ok") == 1.0;
    }


    public MongoDBCollection getCollection(String collection) {
        if(!mongoDBCollections.containsKey(collection)) {
            MongoDBCollection mongoDBCollection = new MongoDBCollection(db.getCollection(collection));
            mongoDBCollections.put(collection, mongoDBCollection);
            logger.debug("MongoDataStore: new MongoDB collection '{}' created", collection);
        }
        return mongoDBCollections.get(collection);
    }

    public MongoDBCollection createCollection(String collectionName) {
        List<String> collectionNames = new ArrayList<>();
        db.listCollectionNames().forEach((Consumer<? super String>) name -> collectionNames.add(name));
        if(!collectionNames.contains(collectionName)) {
            db.createCollection(collectionName);
        }
        return getCollection(collectionName);
    }

    public void dropCollection(String collectionName) {
        List<String> collectionNames = new ArrayList<>();
        db.listCollectionNames().forEach((Consumer<? super String>) collectionNames::add);
        if(collectionNames.contains(collectionName)) {
            db.getCollection(collectionName).drop();
            mongoDBCollections.remove(collectionName);
        }
    }

    public List<String> getCollectionNames() {
        Iterator<String> iterator = db.listCollectionNames().iterator();
        List<String> list = new ArrayList<>();
        while(iterator.hasNext()) {
            list.add(iterator.next());
        }
        return list;
    }

    public Document getStats(String collectionName) {
        return db.runCommand(new BsonDocument("dbStats", new BsonInt32(1)));
    }


    void close() {
        logger.info("MongoDataStore: connection closed");
        mongoClient.close();
    }


    /*
     * GETTERS, NO SETTERS ARE AVAILABLE TO MAKE THIS CLASS IMMUTABLE
     */

    public Map<String, MongoDBCollection> getMongoDBCollections() {
        return mongoDBCollections;
    }

    public MongoDatabase getDb() {
        return db;
    }

    public String getDatabaseName() {
        return db.getName();
    }

    public MongoDBConfiguration getMongoDBConfiguration() {
        return mongoDBConfiguration;
    }

}
