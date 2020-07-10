package org.opencb.datastore.mongodb;

import org.bson.Document;
import org.junit.*;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Created by imedina on 13/04/14.
 */
public class MongoDataStoreTest {

    public static final String UNIT_TEST_COLLECTION_NAME = "JUnitTest";

    private static MongoDataStoreManager mongoDataStoreManager;
    private static MongoDataStore mongoDataStore;

    @BeforeClass
    public static void setUp() throws Exception {
        mongoDataStoreManager = new MongoDataStoreManager("localhost", 27017);
        mongoDataStore = mongoDataStoreManager.get("test");
        mongoDataStore.createCollection(UNIT_TEST_COLLECTION_NAME);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        mongoDataStoreManager.close("test");
    }


    @Test
    public void testDataStoreTestLogic() throws Exception {
        assertTrue(mongoDataStore.test());
    }

    @Test
    public void testGetCollection() throws Exception {
        MongoDBCollection mongoDBCollection = mongoDataStore.getCollection(UNIT_TEST_COLLECTION_NAME);
        assertTrue(mongoDBCollection != null);
    }

    @Test
    public void testCreateCollection() throws Exception {
        MongoDBCollection mongoDBCollection = mongoDataStore.getCollection(UNIT_TEST_COLLECTION_NAME);
        assertEquals(0L, (long) mongoDBCollection.count().getResult().get(0));
    }

    @Test
    public void testDropCollection() throws Exception {
        final String tempCollectionName = "tempCollection";
        mongoDataStore.createCollection(tempCollectionName);
        assertNotNull(mongoDataStore.getCollection(tempCollectionName));
        mongoDataStore.dropCollection(tempCollectionName);
        assertFalse(mongoDataStore.getCollectionNames().contains(tempCollectionName));
    }

    @Test
    public void testGetCollectionNames() throws Exception {
        List<String> colNames = mongoDataStore.getCollectionNames();
        assertTrue(colNames.size() > 0);
    }

    @Test
    public void testGetStats() throws Exception {
        Document statsDocument = mongoDataStore.getStats(UNIT_TEST_COLLECTION_NAME);
        assertEquals(1.0, statsDocument.getDouble("ok"), 0.0);
    }
}
