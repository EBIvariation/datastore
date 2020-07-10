package org.opencb.datastore.mongodb;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;

import com.mongodb.*;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

import org.bson.BsonInt32;
import org.bson.Document;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.opencb.datastore.core.ComplexTypeConverter;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.datastore.core.QueryResultWriter;

import static org.junit.Assert.*;

/**
 * Created by imedina on 29/03/14.
 */
public class MongoDBCollectionTest {

    private static MongoDataStoreManager mongoDataStoreManager;
    private static MongoDataStore mongoDataStore;
    private static MongoDBCollection mongoDBCollection;
    private static MongoDBCollection mongoDBCollectionInsertTest;
    private static MongoDBCollection mongoDBCollectionUpdateTest;
    private static MongoDBCollection mongoDBCollectionRemoveTest;

    private static int N = 1000;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void beforeClass() throws Exception {
        mongoDataStoreManager = new MongoDataStoreManager("localhost", 27017);
        mongoDataStore = mongoDataStoreManager.get("datastore_test");

        mongoDBCollection = createTestCollection("test", N);
        mongoDBCollectionInsertTest = createTestCollection("insert_test", 50);
        mongoDBCollectionUpdateTest = createTestCollection("update_test", 50);
        mongoDBCollectionRemoveTest = createTestCollection("remove_test", 50);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        mongoDataStoreManager.drop("datastore_test");
        mongoDataStore.close();
    }

    private static MongoDBCollection createTestCollection(String test, int size) {
        MongoDBCollection mongoDBCollection = mongoDataStore.getCollection(test);
        Document document;
        for(int i = 0; i < size; i++) {
            document = new Document("id", i);
            document.put("name", "John");
            document.put("surname", "Doe");
            document.put("age", i % 5);
            mongoDBCollection.nativeQuery().insert(document, null);
        }
        return mongoDBCollection;
    }

    @Test
    public void testQueryResultWriter() throws Exception {
        QueryOptions queryOptions = new QueryOptions();
        MongoDBCollection collection = mongoDataStore.createCollection("testQueryResultWriter");
        queryOptions.add("w", 0);
        for (int i = 0; i < 100; i++) {
            collection.insert(new Document("id", new BsonInt32(i)), queryOptions);
        }

        BasicQueryResultWriter queryResultWriter = new BasicQueryResultWriter();
        collection.setQueryResultWriter(queryResultWriter);
        QueryResult<Document> documentQueryResult = collection.find(new Document("id", new Document("$gt", 50)), null);
        assert (documentQueryResult.getResult().isEmpty());

        collection.setQueryResultWriter(null);
        documentQueryResult = collection.find(new Document("id", new Document("$gt", 50)), null);
        assertEquals (49, documentQueryResult.getResult().size());
    }

    @Test
    public void testDistinct() throws Exception {
        QueryResult<Integer> id1 = mongoDBCollection.distinct("id", Integer.class);
        assertTrue(id1.getResult().stream().allMatch(id -> id < 1000));
        QueryResult<String> id2 = mongoDBCollection.distinct("id", null, Integer.class, new ComplexTypeConverter<String, Integer>() {
            @Override
            public String convertToDataModelType(Integer intValue) {
                return intValue.toString();
            }
            @Override
            public Integer convertToStorageType(String stringValue) {
                Integer intValue;
                try {
                    intValue = Integer.parseInt(stringValue);
                    return intValue;
                }
                catch(Exception ex)
                {
                    System.out.println("Non integer result : " + stringValue);
                    return 0;
                }
            }
        });
        assertTrue(id2.getResult().stream().allMatch(id -> Integer.parseInt(id) < 1000));
    }

    @Test
    public void testCount() throws Exception {
        QueryResult<Long> queryResult = mongoDBCollection.count();
        assertEquals("The number of documents must be equals", new Long(N), queryResult.getResult().get(0));
    }

    @Test
    public void testCount1() throws Exception {
        QueryResult<Long> queryResult = mongoDBCollection.count();
        assertEquals("The number must be equals", new Long(N), queryResult.first());
    }

    @Test
    public void testDistinct1() throws Exception {
        QueryResult<Integer> queryResult = mongoDBCollection.distinct("age", null, Integer.class);
        assertNotNull("Age cannot be null", queryResult);
        assertEquals("ResultType must be 'java.lang.Integer'", "java.lang.Integer", queryResult.getResultType());
    }

    @Test
    public void testDistinct2() throws Exception {
        QueryResult<String> queryResult = mongoDBCollection.distinct("name", null, String.class);
        assertNotNull("Object cannot be null", queryResult);
        assertEquals("ResultType must be 'java.lang.String'", "java.lang.String", queryResult.getResultType());
    }

    @Test
    public void testDistinct3() throws Exception {
        QueryResult<String> queryResult = mongoDBCollection.distinct("age", null, Integer.class, new ComplexTypeConverter<String, Integer>() {
            @Override
            public String convertToDataModelType(Integer intvalue) {
                return intvalue.toString();
            }

            @Override
            public Integer convertToStorageType(String stringValue) {
                return Integer.parseInt(stringValue);
            }
        });
        assertNotNull("Object cannot be null", queryResult);
        assertEquals("ResultType must be 'java.lang.String'", "java.lang.String", queryResult.getResultType());
    }

    @Test
    public void testFind() throws Exception {
        Document document = new Document("id", 4);
        QueryOptions queryOptions = new QueryOptions("include", Arrays.asList("id"));
        QueryResult<Document> queryResult = mongoDBCollection.find(document, queryOptions);
        assertNotNull("Object cannot be null", queryResult.getResult());
        assertEquals("Returned Id does not match", 4, queryResult.first().get("id"));
//        System.out.println("queryResult 'include' = " + queryResult);
    }

    @Test
    public void testFind1() throws Exception {
        Document document = new Document("id", 4);
        Document returnFields = new Document("id", 1);
        QueryOptions queryOptions = new QueryOptions("exclude", Arrays.asList("id"));
        QueryResult<Document> queryResult = mongoDBCollection.find(document, returnFields, queryOptions);
        assertNotNull("Object cannot be null", queryResult.getResult());
        assertNull("Field 'name' must not exist", queryResult.first().get("name"));
//        System.out.println("queryResult 'projection' = " + queryResult);
    }

    @Test
    public void testFind2() throws Exception {
        Document document = new Document("id", 4);
        Document returnFields = new Document("id", 1);
        QueryOptions queryOptions = new QueryOptions("exclude", Arrays.asList("id"));
        QueryResult<HashMap> queryResult = mongoDBCollection.find(document, returnFields, HashMap.class, queryOptions);
        assertNotNull("Object cannot be null", queryResult.getResult());
        assertTrue("Returned field must instance of Hashmap", queryResult.first() instanceof HashMap);
    }

    @Test
    public void testFind3() throws Exception {
        final Document findCriteria = new Document("id", 4);
        Document returnFields = new Document("id", 1);
        QueryOptions queryOptions = new QueryOptions("exclude", Arrays.asList("id"));
        QueryResult<HashMap> queryResult = mongoDBCollection.find(findCriteria, returnFields,
                new ComplexTypeConverter<HashMap, Document>() {
            @Override
            public HashMap convertToDataModelType(Document document) {
                return new HashMap(new BasicDBObject(document).toMap());
            }

            @Override
            public Document convertToStorageType(HashMap object) {
                return null;
            }
        }, queryOptions);
        assertNotNull("Object cannot be null", queryResult.getResult());
        assertTrue("Returned field must instance of Hashmap", queryResult.first() instanceof HashMap);
    }

    @Test
    public void testFind4() throws Exception {
        List<Document> documentList = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            documentList.add(new Document("id", i));
        }
        QueryOptions queryOptions = new QueryOptions("include", Arrays.asList("id"));
        List<QueryResult<Document>> queryResultList = mongoDBCollection.find(documentList, queryOptions);
        assertEquals("List must contain 10 results", 10, queryResultList.size());
        assertNotNull("Object cannot be null", queryResultList.get(0).getResult());
        assertEquals("Returned Id does not match", 9, queryResultList.get(9).first().get("id"));
    }

    @Test
    public void testFind5() throws Exception {
        List<Document> documentList = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            documentList.add(new Document("id", i));
        }
        Document returnFields = new Document("id", 1);
        QueryOptions queryOptions = new QueryOptions("exclude", Arrays.asList("id"));
        List<QueryResult<Document>> queryResultList = mongoDBCollection.find(documentList, returnFields, queryOptions);
        assertEquals("List must contain 10 results", 10, queryResultList.size());
        assertNotNull("Object cannot be null", queryResultList.get(0).getResult());
        assertNull("Field 'name' must not exist", queryResultList.get(0).first().get("name"));
        assertEquals("resultType must be 'org.bson.Document'", "org.bson.Document",
                     queryResultList.get(0).getResultType());
    }

    @Test
    public void testFind6() throws Exception {
        List<Document> documentList = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            documentList.add(new Document("id", i));
        }
        Document returnFields = new Document("id", 1);
        QueryOptions queryOptions = new QueryOptions("exclude", Arrays.asList("id"));
        List<QueryResult<HashMap>> queryResultList = mongoDBCollection.find(documentList, returnFields, HashMap.class, queryOptions);
        assertNotNull("Object queryResultList cannot be null", queryResultList);
        assertNotNull("Object queryResultList.get(0) cannot be null", queryResultList.get(0).getResult());
        assertTrue("Returned field must instance of HashMap", queryResultList.get(0).first() instanceof HashMap);
        assertEquals("resultType must 'java.util.HashMap'", "java.util.HashMap", queryResultList.get(0).getResultType());
    }

    @Test
    public void testFind7() throws Exception {
        final List<Document> documentList = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            documentList.add(new Document("id", i));
        }
        Document returnFields = new Document("id", 1);
        QueryOptions queryOptions = new QueryOptions("exclude", Arrays.asList("id"));
        List<QueryResult<HashMap>> queryResultList = mongoDBCollection.find(documentList, returnFields, new ComplexTypeConverter<HashMap, Document>() {
            @Override
            public HashMap convertToDataModelType(Document document) {
                return new HashMap(new BasicDBObject(document).toMap());
            }

            @Override
            public Document convertToStorageType(HashMap object) {
                return null;
            }
        }, queryOptions);
        assertNotNull("Object queryResultList cannot be null", queryResultList);
        assertNotNull("Object queryResultList.get(0) cannot be null", queryResultList.get(0).getResult());
        assertTrue("Returned field must instance of Hashmap", queryResultList.get(0).first() instanceof HashMap);
        assertEquals("resultType must 'java.util.HashMap'", "java.util.HashMap", queryResultList.get(0).getResultType());
    }

    @Test
    public void testAggregate() throws Exception {
        List<Document> documentList = new ArrayList<>();
        Document match = new Document("$match", new Document("age", new Document("$gt", 2)));
        Document group = new Document("$group", new Document("_id", "$age"));

        documentList.add(match);
        documentList.add(group);

        QueryResult queryResult = mongoDBCollection.aggregate(documentList, null);
        assertNotNull("Object queryResult cannot be null", queryResult);
        assertNotNull("Object queryResult.getResult() cannot be null", queryResult.getResult());
        assertEquals("There must be 2 results", 2, queryResult.getResult().size());
    }

    @Test
    public void testInsert() throws Exception {
        Long countBefore = mongoDBCollectionInsertTest.count().first();
        for (int i = 1; i < 50; i++) {
            mongoDBCollectionInsertTest.insert(new Document("insertedObject", i), null);
            assertEquals("Insert operation must insert 1 element each time.", countBefore + i, mongoDBCollectionInsertTest.count().first().longValue()  );
        }
    }

    @Test
    public void testInsert1() throws Exception {
        Document uniqueDocument = new Document("_id", "myUniqueId");
        mongoDBCollectionInsertTest.insert(uniqueDocument, null);

        thrown.expect(MongoWriteException.class);
        thrown.expectMessage("E11000 duplicate key error collection");
        mongoDBCollectionInsertTest.insert(uniqueDocument, null);
    }

    @Test
    public void testInsert2() throws Exception {
        Long countBefore = mongoDBCollectionInsertTest.count().first();
        int numBulkInsertions = 50;
        int bulkInsertSize = 100;

        for (int b = 1; b < numBulkInsertions; b++) {
            ArrayList<Document> list = new ArrayList<>(bulkInsertSize);
            for (int i = 0; i < bulkInsertSize; i++) {
                list.add(new Document("bulkInsertedObject", i));
            }
            mongoDBCollectionInsertTest.insert(list, null);
            assertEquals("Bulk insert operation must insert " + bulkInsertSize + " elements each time.", countBefore + bulkInsertSize * b, mongoDBCollectionInsertTest.count().first().longValue());
        }
    }

    @Test
    public void testInsert3() throws Exception {
        Document uniqueDocument = new Document("_id", "myUniqueId");

        ArrayList<Document> list = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            list.add(uniqueDocument);
        }

        thrown.expect(MongoBulkWriteException.class);
        mongoDBCollectionInsertTest.insert(list, null);
    }

    @Test
    public void testUpdate() throws Exception {
        Document query = new Document("name", "John");
        long count = mongoDBCollectionUpdateTest.count(query).first();
        UpdateResult updateResult = mongoDBCollectionUpdateTest.update(query,
                                                                       new Document("$set", new Document("modified", true)),
                                                                       new QueryOptions("multi", true)
        ).first();
        assertEquals("All the objects are named \"John\", so all objects should be modified", count,
                     updateResult.getModifiedCount());
    }

    @Test
    public void testUpdate1() throws Exception {
        UpdateResult updateResult = mongoDBCollectionUpdateTest.update(new Document("surname", "Johnson"),
                new Document("$set", new Document("modifiedAgain", true)),
                new QueryOptions("multi", true)
        ).first();
        assertEquals("Any objects have the surname \"Johnson\", so any objects should be modified", 0,
                     updateResult.getModifiedCount());
    }

    @Test
    public void testUpdate2() throws Exception {
        UpdateResult updateResult = mongoDBCollectionUpdateTest.update(new Document("surname", "Johnson"),
                new Document("$set", new Document("modifiedAgain", true)),
                new QueryOptions("upsert", true)
        ).first();
        assertEquals("Any objects have the surname \"Johnson\", so one object should be inserted", true,
                     updateResult.getUpsertedId() != null);
    }

    @Test
    public void testUpdate3() throws Exception {
        int count = mongoDBCollectionUpdateTest.count().first().intValue();
        int modifiedDocuments = count / 2;
        ArrayList<Document> queries = new ArrayList<>(modifiedDocuments);
        ArrayList<Document> updates = new ArrayList<>(modifiedDocuments);

        for (int i = 0; i < modifiedDocuments; i++) {
            queries.add(new Document("id", i));
            updates.add(new Document("$set", new Document("bulkUpdated", i)));
        }
        BulkWriteResult bulkWriteResult = mongoDBCollectionUpdateTest.update(queries, updates, new QueryOptions("multi", false)).first();
        assertEquals("", modifiedDocuments, bulkWriteResult.getModifiedCount());
    }

    @Test
    public void testUpdate4() throws Exception {
        int count = mongoDBCollectionUpdateTest.count().first().intValue();
        int modifiedDocuments = count / 2;
        ArrayList<Document> queries = new ArrayList<>(modifiedDocuments);
        ArrayList<Document> updates = new ArrayList<>(modifiedDocuments);

        for (int i = 0; i < modifiedDocuments; i++) {
            queries.add(new Document("id", i));
            updates.add(new Document("$set", new Document("bulkUpdated", i)));
        }
        updates.remove(updates.size()-1);

        thrown.expect(IndexOutOfBoundsException.class);
        mongoDBCollectionUpdateTest.update(queries, updates, new QueryOptions("multi", false));
    }

    @Test
    public void testRemove() throws Exception {
        int count = mongoDBCollectionRemoveTest.count().first().intValue();
        Document query = new Document("age", 1);
        int numDeletions = mongoDBCollectionRemoveTest.count(query).first().intValue();
        DeleteResult deleteResult = mongoDBCollectionRemoveTest.remove(query, null).first();
        assertEquals(numDeletions, deleteResult.getDeletedCount());
        assertEquals(mongoDBCollectionRemoveTest.count().first().intValue(), count - numDeletions);
    }

    @Test
    public void testRemove1() throws Exception {
        int count = mongoDBCollectionRemoveTest.count().first().intValue();

        int numDeletions = 10;
        List<Document> remove = new ArrayList<>(numDeletions);
        for (int i = 0; i < numDeletions; i++) {
            remove.add(new Document("name", "John"));
        }

        BulkWriteResult bulkWriteResult = mongoDBCollectionRemoveTest.remove(remove, null).first();
        assertEquals(numDeletions, bulkWriteResult.getDeletedCount());
        assertEquals(mongoDBCollectionRemoveTest.count().first().intValue(), count - numDeletions);
    }

    @Test
    public void testFindAndModify() throws Exception {

    }

    @Test
    public void testFindAndModify1() throws Exception {

    }

    @Test
    public void testFindAndModify2() throws Exception {

    }

    @Test
    public void testCreateIndex() throws Exception {
        mongoDBCollection.createIndex(
                new Document("surname", 1),
                new Document("name", "idIndex")
                        .append("unique", false)
                        .append("background", true)
                        .append("version", 2)
                        .append("partialFilterExpression", new Document("age", new Document("$gte", 3)))
        );
        List<Document> indexes = mongoDBCollection.getIndex().getResult();
        Optional<Document> idIndex = indexes.stream().filter(index -> index.getString("name").equals("idIndex"))
                                            .findFirst();
        assertTrue(idIndex.isPresent());
        assertFalse(idIndex.get().getBoolean("unique", false));
        assertTrue(idIndex.get().getBoolean("background", false));
        assertEquals(2, (int) idIndex.get().getInteger("v"));
        assertTrue(((Document)idIndex.get().get("partialFilterExpression", new Document())).containsKey("age"));
    }

    @Test
    public void testDropIndex() throws Exception {

    }

    @Test
    public void testGetIndex() throws Exception {

    }

    class BasicQueryResultWriter implements QueryResultWriter<Document> {
        int i = 0;
        String outfile = "/tmp/queryResultWriter.log";
        DataOutputStream fileOutputStream;

        @Override
        public void open() throws IOException {
            System.out.println("Opening!");
            this.fileOutputStream = new DataOutputStream(new FileOutputStream(outfile));
        }

        @Override
        public void write(Document elem) throws IOException {
            String s = String.format("Result %d : %s\n", i++, elem.toString());
            System.out.printf(s);
            fileOutputStream.writeBytes(s);
        }

        @Override
        public void close() throws IOException {
            System.out.println("Closing!");
            fileOutputStream.close();
        }
    }

}
