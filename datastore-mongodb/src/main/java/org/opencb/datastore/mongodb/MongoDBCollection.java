package org.opencb.datastore.mongodb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.*;
import com.mongodb.bulk.BulkWriteResult;

import java.io.IOException;
import java.util.*;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.opencb.datastore.core.ComplexTypeConverter;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.datastore.core.QueryResultWriter;

/**
 * @author Ignacio Medina &lt;imedina@ebi.ac.uk&gt;
 * @author Cristina Yenyxe Gonzalez Garcia &lt;cyenyxe@ebi.ac.uk&gt;
 */
public class MongoDBCollection {

    private MongoCollection dbCollection;

    private long start;
    private long end;

    private MongoDBNativeQuery mongoDBNativeQuery;
    private QueryResultWriter<Document> queryResultWriter;

    private ObjectMapper objectMapper;

    MongoDBCollection(MongoCollection dbCollection) {
        this(dbCollection, null);
    }

    MongoDBCollection(MongoCollection dbCollection, QueryResultWriter<Document> queryResultWriter) {
        this.dbCollection = dbCollection;
        this.queryResultWriter = queryResultWriter;

        mongoDBNativeQuery = new MongoDBNativeQuery(dbCollection);

        objectMapper = new ObjectMapper();
    }


    private void startQuery() {
        start = System.currentTimeMillis();
    }

    private <T> QueryResult<T> endQuery(List result) {
        int numResults = (result != null) ? result.size() : 0;
        return endQuery(result, numResults);
    }

    private <T> QueryResult<T> endQuery(List result, int numTotalResults) {
        end = System.currentTimeMillis();
        int numResults = (result != null) ? result.size() : 0;

        QueryResult<T> queryResult = new QueryResult(null, (int) (end-start), numResults, numTotalResults, null, null, result);
        // If a converter is provided, convert Documents to the requested type
//        if (converter != null) {
//            List convertedResult = new ArrayList<>(numResults);
//            for (Object o : result) {
//                convertedResult.add(converter.convertToDataModelType(o));
//            }
//            queryResult.setResult(convertedResult);
//        } else {
//            queryResult.setResult(result);
//        }

        return queryResult;

    }

    public QueryResult<Long> count() {
        startQuery();
        long l = mongoDBNativeQuery.count();
        return endQuery(Arrays.asList(l));
    }

    public QueryResult<Long> count(Document query) {
        startQuery();
        long l = mongoDBNativeQuery.count(query);
        return endQuery(Arrays.asList(l));
    }

    public <T> QueryResult<T> distinct(String key, Class<T> clazz) {
        return distinct(key, null, clazz);
    }

    public <T> QueryResult<T> distinct(String key, Document query, Class<T> clazz) {
        startQuery();
        List<T> l = mongoDBNativeQuery.distinct(key, query, clazz);
        return endQuery(l);
    }

    public <T, O> QueryResult<T> distinct(String key, Document query, Class<O> clazz,
                                          ComplexTypeConverter<T, O> converter) {
        startQuery();
        List<O> distinct = mongoDBNativeQuery.distinct(key, query, clazz);

        List<T> convertedresultList = new ArrayList<>(distinct.size());
        for (O o : distinct) {
            convertedresultList.add(converter.convertToDataModelType(o));
        }
        return endQuery(convertedresultList);
    }



    public QueryResult<Document> find(Document query, QueryOptions options) {
        return _find(query, null, Document.class, null, options);
    }

    public QueryResult<Document> find(Document query, Document projection, QueryOptions options) {
        return _find(query, projection, Document.class, null, options);
    }

    public <T> QueryResult<T> find(Document query, Document projection, Class<T> clazz, QueryOptions options) {
        return _find(query, projection, clazz, null, options);
    }

    public <T> QueryResult<T> find(Document query, Document projection, ComplexTypeConverter<T, Document> converter,
                                   QueryOptions options) {
        return _find(query, projection, null, converter, options);
    }


    public List<QueryResult<Document>> find(List<Document> queries, QueryOptions options) {
        return find(queries, null, options);
    }

    public List<QueryResult<Document>> find(List<Document> queries, Document projection, QueryOptions options) {
        return _find(queries, projection, null, null, options);
    }

    public <T> List<QueryResult<T>> find(List<Document> queries, Document projection, Class<T> clazz,
                                         QueryOptions options) {
        return _find(queries, projection, clazz, null, options);
    }

    public <T> List<QueryResult<T>> find(List<Document> queries, Document projection,
                                         ComplexTypeConverter<T, Document> converter, QueryOptions options) {
        return _find(queries, projection, null, converter, options);
    }

    public <T> List<QueryResult<T>> _find(List<Document> queries, Document projection, Class<T> clazz,
                                          ComplexTypeConverter<T, Document> converter, QueryOptions options) {
        List<QueryResult<T>> queryResultList = new ArrayList<>(queries.size());
        for(Document query: queries) {
            QueryResult<T> queryResult = _find(query, projection, clazz, converter, options);
            queryResultList.add(queryResult);
        }
        return  queryResultList;
    }

    private <T> QueryResult<T> _find(Document query, Document projection, Class<T> clazz,
                                     ComplexTypeConverter<T, Document> converter, QueryOptions options) {
        startQuery();

        /*
         * Getting the cursor and setting the batchSize from options. Default value set to 20.
         */
        FindIterable<Document> cursor = mongoDBNativeQuery.find(query, projection, options);
        if(options != null && options.containsKey("batchSize")) {
            cursor.batchSize(options.getInt("batchSize"));
        }else {
            cursor.batchSize(20);
        }

        QueryResult<T> queryResult;
        List<T> list = new LinkedList<>();
        int numDocuments = 0;
        if (queryResultWriter != null) {
            try {
                queryResultWriter.open();
                for(Document document: cursor) {
                    numDocuments += 1;
                    queryResultWriter.write(document);
                }
                queryResultWriter.close();
            } catch (IOException e) {
                queryResult = endQuery(null);
                queryResult.setErrorMsg(e.getMessage() + " " + Arrays.toString(e.getStackTrace()));
                return queryResult;
            }
        } else {
            if(converter != null) {
                for(Document document: cursor) {
                    numDocuments += 1;
                    list.add(converter.convertToDataModelType(document));
                }
            }else {
                if(clazz != null && !clazz.equals(Document.class)) {
                    for(Document document: cursor) {
                        try {
                            list.add(objectMapper.readValue(document.toJson(), clazz));
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                } else {
                    for(Document document: cursor) {
                        list.add((T) document);
                    }
                }
            }
        }

        if (options != null && options.getInt("limit") > 0) {
            queryResult = endQuery(list, numDocuments);
        } else {
            queryResult = endQuery(list);
        }

        if (numDocuments == 0) {
            queryResult = endQuery(list);
        }

        return queryResult;
    }


    public QueryResult<Document> aggregate(List<Document> operations, QueryOptions options) {
        startQuery();
        QueryResult<Document> queryResult;
        AggregateIterable<Document> output = mongoDBNativeQuery.aggregate(operations, options);
        Iterator<Document> iterator = output.iterator();
        List<Document> list = new LinkedList<>();
        if (queryResultWriter != null) {
            try {
                queryResultWriter.open();
                while (iterator.hasNext()) {
                    queryResultWriter.write(iterator.next());
                }
                queryResultWriter.close();
            } catch (IOException e) {
                queryResult = endQuery(list);
                queryResult.setErrorMsg(e.getMessage() + " " + Arrays.toString(e.getStackTrace()));
                return queryResult;
            }
        } else {
            while (iterator.hasNext()) {
                list.add(iterator.next());
            }
        }
        queryResult = endQuery(list);
        return queryResult;
    }


    public QueryResult<WriteResult> insert(Document object, QueryOptions options) {
        startQuery();
        WriteResult wr = mongoDBNativeQuery.insert(object, options);
        QueryResult<WriteResult> queryResult = endQuery(Arrays.asList(wr));
        return queryResult;
    }

    //Bulk insert
    public QueryResult<BulkWriteResult> insert(List<Document> objects, QueryOptions options) {
        startQuery();
        BulkWriteResult writeResult = mongoDBNativeQuery.insert(objects, options);
        QueryResult<BulkWriteResult> queryResult = endQuery(Collections.singletonList(writeResult));
        return queryResult;
    }


    public QueryResult<UpdateResult> update(Document query, Document update, QueryOptions options) {
        startQuery();

        boolean upsert = false;
        boolean multi = false;
        if(options != null) {
            upsert = options.getBoolean("upsert");
            multi = options.getBoolean("multi");
        }

        UpdateResult ur = mongoDBNativeQuery.update(query, update, upsert, multi);
        QueryResult<UpdateResult> queryResult = endQuery(Arrays.asList(ur));
        return queryResult;
    }

    //Bulk update
    public QueryResult<BulkWriteResult> update(List<Document> queries, List<Document> updates, QueryOptions options) {
        startQuery();

        boolean upsert = false;
        boolean multi = false;
        if(options != null) {
            upsert = options.getBoolean("upsert");
            multi = options.getBoolean("multi");
        }

        BulkWriteResult wr = mongoDBNativeQuery.update(queries, updates, upsert, multi);
        QueryResult<BulkWriteResult> queryResult = endQuery(Arrays.asList(wr));
        return queryResult;
    }


    public QueryResult<DeleteResult> remove(Document query, QueryOptions options) {
        startQuery();
        DeleteResult dr = mongoDBNativeQuery.remove(query);
        QueryResult<DeleteResult> queryResult = endQuery(Arrays.asList(dr));
        return queryResult;
    }

    //Bulk remove
    public QueryResult<BulkWriteResult> remove(List<Document> query, QueryOptions options) {
        startQuery();

        boolean multi = false;
        if(options != null) {
            multi = options.getBoolean("multi");
        }
        BulkWriteResult wr = mongoDBNativeQuery.remove(query, multi);
        QueryResult<BulkWriteResult> queryResult = endQuery(Arrays.asList(wr));

        return queryResult;
    }



    public QueryResult<Document> findAndModify(Document query, Document fields, Document sort, Document update,
                                               QueryOptions options) {
        return _findAndModify(query, fields, sort, update, options, null, null);
    }

    public <T> QueryResult<T> findAndModify(Document query, Document fields, Document sort, Document update,
                                            QueryOptions options, Class<T> clazz) {
        return _findAndModify(query, fields, sort, update, options, clazz, null);
    }

    public <T> QueryResult<T> findAndModify(Document query, Document fields, Document sort, Document update,
                                            QueryOptions options, ComplexTypeConverter<T, Document> converter) {
        return _findAndModify(query, fields, sort, update, options, null, converter);
    }

    private <T> QueryResult<T> _findAndModify(Document query, Document fields, Document sort, Document update,
                                              QueryOptions options, Class<T> clazz, ComplexTypeConverter<T, Document> converter) {
        startQuery();
        Document result = mongoDBNativeQuery.findAndModify(query, fields, sort, update, options);
        QueryResult<T> queryResult = endQuery(Arrays.asList(result));

        return queryResult;
    }



    public QueryResult createIndex(Document keys, Document options) {
        startQuery();
        mongoDBNativeQuery.createIndex(keys, options);
        QueryResult queryResult = endQuery(Collections.emptyList());
        return queryResult;
    }

    public QueryResult dropIndex(Document keys) {
        startQuery();
        mongoDBNativeQuery.dropIndex(keys);
        QueryResult queryResult = endQuery(Collections.emptyList());
        return queryResult;
    }

    public QueryResult<Document> getIndex() {
        startQuery();
        List<Document> index = mongoDBNativeQuery.getIndex();
        QueryResult<Document> queryResult = endQuery(index);
        return queryResult;
    }



    public QueryResultWriter<Document> getQueryResultWriter() {
        return queryResultWriter;
    }

    public void setQueryResultWriter(QueryResultWriter queryResultWriter) {
        this.queryResultWriter = queryResultWriter;
    }

    /**
     * Returns a Native instance to MongoDB. This is a convenience method,
     * equivalent to {@code new MongoClientOptions.Native()}.
     *
     * @return a new instance of a Native
     */
    public MongoDBNativeQuery nativeQuery() {
        return mongoDBNativeQuery;
    }

}
