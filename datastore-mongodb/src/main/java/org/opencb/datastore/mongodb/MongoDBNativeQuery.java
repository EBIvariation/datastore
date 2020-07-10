package org.opencb.datastore.mongodb;

import com.mongodb.MongoWriteException;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import com.mongodb.bulk.BulkWriteResult;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.DeleteManyModel;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.opencb.datastore.core.QueryOptions;


/**
 * Created by imedina on 28/03/14.
 */
public class MongoDBNativeQuery {

    private final MongoCollection<Document> dbCollection;

    MongoDBNativeQuery(MongoCollection<Document> dbCollection) {
        this.dbCollection = dbCollection;
    }

    public long count() {
        return dbCollection.countDocuments();
    }

    public long count(Document query) {
        return dbCollection.countDocuments(query);
    }

    public <T> List<T> distinct(String key, final Class<T> clazz) {
        return distinct(key, null, clazz);
    }

    public <T> List<T> distinct(String key, Document query, final Class<T> clazz) {
        List<T> result = new ArrayList<>();
        DistinctIterable<T> distinctIterable;
        if (query != null) {
            distinctIterable = dbCollection.distinct(key, query, clazz);
        } else {
            distinctIterable = dbCollection.distinct(key, clazz);
        }
        for (T obj: distinctIterable) {
            result.add(obj);
        } ;
        return result;
    }

    public FindIterable<Document> find(Document query, QueryOptions options) {
        return find(query, null, options);
    }

    public FindIterable<Document> find(Document query, Document projection, QueryOptions options) {
        FindIterable<Document> cursor;

        if(projection == null) {
            projection = getProjection(projection, options);
        }
        cursor = dbCollection.find(query).projection(projection);

        int limit = (options != null) ? options.getInt("limit", 0) : 0;
        if (limit > 0) {
            cursor.limit(limit);
        }

        int skip = (options != null) ? options.getInt("skip", 0) : 0;
        if (skip > 0) {
            cursor.skip(skip);
        }

        Document sort = (options != null) ? (Document) options.get("sort") : null;
        if (sort != null) {
            cursor.sort(sort);
        }

        return cursor;
    }

    public AggregateIterable<Document> aggregate(List<Document> operations, QueryOptions options) {
        return (operations.size() > 0) ? dbCollection.aggregate(operations) : null;
    }

    /**
     * This method insert a single document into a collection. Params w and wtimeout are read from QueryOptions.
     * @param document
     * @param options
     * @return
     */
    public WriteResult insert(Document document, QueryOptions options) throws MongoWriteException {
        WriteConcern writeConcernToUse = getWriteConcern(options);
        dbCollection.withWriteConcern(writeConcernToUse).insertOne(document);
        return new WriteResult(1, false, document.get("_id"));
    }

    private WriteConcern getWriteConcern(QueryOptions options) {
        WriteConcern writeConcernToUse;
        if(options != null && (options.containsKey("w") || options.containsKey("wtimeout"))) {
            // Some info about params: http://api.mongodb.org/java/current/com/mongodb/WriteConcern.html
            writeConcernToUse = new WriteConcern(options.getInt("w", 1),
                                                 options.getInt("wtimeout", 0));
        } else {
            writeConcernToUse = WriteConcern.MAJORITY;
        }
        return writeConcernToUse;
    }

    /**
     * This method insert a list of documents into a collection. Params w and wtimeout are read from QueryOptions.
     * @param documentList
     * @param options
     * @return
     */
    public BulkWriteResult insert(List<Document> documentList, QueryOptions options) {
        // Let's prepare the Bulk object
        List<WriteModel<Document>> insertRequests = new ArrayList<>();
        for (Document document : documentList) {
            insertRequests.add(new InsertOneModel<>(document));
        }
        return dbCollection.withWriteConcern(getWriteConcern(options)).bulkWrite(insertRequests);
    }

    public UpdateResult update(Document object, Document updates, boolean upsert, boolean multi) {
        UpdateOptions updateOptions = new UpdateOptions().upsert(upsert);
        if (multi) {
            return dbCollection.updateMany(object, updates, updateOptions);
        } else {
            return dbCollection.updateOne(object, updates, updateOptions);
        }
    }

    public BulkWriteResult update(List<Document> queryList, List<Document> updatesList, boolean upsert, boolean multi) {
        if (queryList.size() != updatesList.size()) {
            throw new IndexOutOfBoundsException("QueryList.size and UpdatesList must be the same size");
        }

        List<WriteModel<Document>> updateRequests = new ArrayList<>();
        UpdateOptions updateOptions = new UpdateOptions().upsert(upsert);

        Iterator<Document> queryIterator = queryList.iterator();
        Iterator<Document> updateIterator = updatesList.iterator();
        while (queryIterator.hasNext()) {
            Document query = queryIterator.next();
            Document update = updateIterator.next();
            if (multi) {
                updateRequests.add(new UpdateManyModel<>(query, update, updateOptions));
            } else {
                updateRequests.add(new UpdateOneModel<>(query, update, updateOptions));
            }
        }
        return dbCollection.bulkWrite(updateRequests);
    }

    public DeleteResult remove(Document query) {
        return dbCollection.deleteMany(query);
    }

    public BulkWriteResult remove(List<Document> queryList, boolean multi) {

        List<WriteModel<Document>> deleteRequests = new ArrayList<>();
        for (Document query : queryList) {
            if (multi) {
                deleteRequests.add(new DeleteManyModel<>(query));
            } else {
                deleteRequests.add(new DeleteOneModel<>(query));
            }
        }
        return dbCollection.bulkWrite(deleteRequests);
    }

    public Document findAndModify(Document query, Document projection, Document sort, Document update,
                                  QueryOptions options) {
        boolean remove = false;
        boolean returnNew = false;
        boolean upsert = false;

        if(options != null) {
            if(projection == null) {
                projection = getProjection(projection, options);
            }
            remove = options.getBoolean("remove", false);
            returnNew = options.getBoolean("returnNew", false);
            upsert = options.getBoolean("upsert", false);
        }
        ReturnDocument returnDocumentOption = returnNew ? ReturnDocument.AFTER : ReturnDocument.BEFORE;
        FindOneAndUpdateOptions findOneAndUpdateOptions =
                new FindOneAndUpdateOptions().projection(projection).sort(sort).returnDocument(returnDocumentOption).upsert(upsert);
        FindOneAndDeleteOptions findOneAndDeleteOptions =
                new FindOneAndDeleteOptions().projection(projection).sort(sort);

        if (remove) {
            return dbCollection.findOneAndDelete(query, findOneAndDeleteOptions);
        } else {
            return dbCollection.findOneAndUpdate(query, update, findOneAndUpdateOptions);
        }
    }

    public void createIndex(Document keys, Document options) {
        IndexOptions indexOptions = new IndexOptions();
        if (options.containsKey("name")) {
            indexOptions = indexOptions.name(options.getString("name"));
        }
        if (options.containsKey("unique")) {
            indexOptions = indexOptions.unique(options.getBoolean("unique"));
        }
        if (options.containsKey("background")) {
            indexOptions = indexOptions.background(options.getBoolean("background"));
        }
        if (options.containsKey("version")) {
            indexOptions = indexOptions.version(options.getInteger("version"));
        }
        if (options.containsKey("partialFilterExpression")) {
            indexOptions = indexOptions.partialFilterExpression((Document)(options.get("partialFilterExpression")));
        }
        dbCollection.createIndex(keys, indexOptions);
    }

    public List<Document> getIndex() {
        List<Document> indexInfoList = new ArrayList<>();
        for (Object indexInfo: dbCollection.listIndexes()) {
            indexInfoList.add((Document)indexInfo);
        }
        return indexInfoList;
    }

    public void dropIndex(Document keys) {
        dbCollection.dropIndex(keys);
    }

    private Document getProjection(Document projection, QueryOptions options) {
        // Select which fields are excluded and included in the query
        if(projection == null) {
            projection = new Document();
        }
        projection.put("_id", 0);

        if (options != null) {
            // Read and process 'include'/'exclude' field from 'options' object
            List<String> includeStringList = options.getAsStringList("include", ",");
            if (includeStringList != null && includeStringList.size() > 0) {
                for (Object field : includeStringList) {
                    projection.put(field.toString(), 1);
                }
            } else {
                List<String> excludeStringList = options.getAsStringList("exclude", ",");
                if (excludeStringList != null && excludeStringList.size() > 0) {
                    for (Object field : excludeStringList) {
                        projection.put(field.toString(), 0);
                    }
                }
            }
        }
        return projection;
    }

}
