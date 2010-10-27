/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.plugin.lruclient;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.count.TransportCountAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.delete.TransportDeleteAction;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.deletebyquery.TransportDeleteByQueryAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.mlt.MoreLikeThisRequest;
import org.elasticsearch.action.mlt.TransportMoreLikeThisAction;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.node.NodeAdminClient;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.threadpool.ThreadPool;

public class LruClient extends NodeClient {
    
    private final ESLogger logger;
    private final LruCache cache;

    private class LruCache extends LinkedHashMap<String, Date> {
        private int capacity = 1;
        public LruCache(int capacity) {
            super(capacity,0.75f,true);
            this.capacity = capacity;
        }

        @Override
        protected boolean removeEldestEntry(Entry<String, Date> eldest) {
            if(size()>capacity) {
                logger.debug("LruCache over capacity: {} removing: {}", this.capacity, eldest.getKey());
                closeIndex(eldest.getKey());
                return true;
            }
            return false;
        }

    }

    @Inject public LruClient(Settings settings, ThreadPool threadPool, NodeAdminClient admin,
                              TransportIndexAction indexAction, TransportDeleteAction deleteAction, TransportBulkAction bulkAction,
                              TransportDeleteByQueryAction deleteByQueryAction, TransportGetAction getAction, TransportCountAction countAction,
                              TransportSearchAction searchAction, TransportSearchScrollAction searchScrollAction,
                              TransportMoreLikeThisAction moreLikeThisAction) {
        super(settings,threadPool,admin,indexAction,deleteAction,bulkAction,deleteByQueryAction,getAction,countAction,searchAction,searchScrollAction,moreLikeThisAction);
        this.logger = Loggers.getLogger(getClass());
        this.cache = new LruCache(settings.getAsInt("node.lrucache.size", 1));
        logger.debug("LruClient",new Exception("DEBUG"));
    }

    private void ensureOpen(final String index) {
        if(!this.cache.containsKey(index)) {
            logger.debug("Index {} not found!  Try to open...", index);
            // force in foreground...
            try {
                admin().indices().prepareOpen(index).execute().actionGet();
            }
            catch(IndexMissingException e) {
                // ignore
                logger.debug("Index {} does not exist!  Can't open.", index);
            }
        }
        this.cache.put(index, new Date());
    }

    private void ensureOpen(final String [] indices) {
        for(int i = 0; i<indices.length; i++)
            ensureOpen(indices[i]);
    }

    private void closeIndex(final String index) {
        // let happen in the background
        logger.debug("Closing index: {} ...", index);
        admin().indices().prepareClose(index).execute().actionGet();
    }

    @Override public void index(IndexRequest request, ActionListener<IndexResponse> listener) {
        logger.debug("index");
        ensureOpen(request.index());
        super.index(request,listener);
    }

    @Override public ActionFuture<DeleteResponse> delete(DeleteRequest request) {
        logger.debug("delete");
        ensureOpen(request.index());
        return super.delete(request);
    }

    @Override public void delete(DeleteRequest request, ActionListener<DeleteResponse> listener) {
        logger.debug("delete");
        ensureOpen(request.index());
        super.delete(request, listener);
    }

    @Override public ActionFuture<BulkResponse> bulk(BulkRequest request) {
        logger.debug("bulk");
        return super.bulk(request);
    }

    @Override public void bulk(BulkRequest request, ActionListener<BulkResponse> listener) {
        logger.debug("bulk");
        super.bulk(request,listener);
    }

    @Override public ActionFuture<DeleteByQueryResponse> deleteByQuery(DeleteByQueryRequest request) {
        logger.debug("deleteByQuery");
        ensureOpen(request.indices());
        return super.deleteByQuery(request);
    }

    @Override public void deleteByQuery(DeleteByQueryRequest request, ActionListener<DeleteByQueryResponse> listener) {
        logger.debug("deleteByQuery");
        ensureOpen(request.indices());
        super.deleteByQuery(request,listener);
    }

    @Override public ActionFuture<GetResponse> get(GetRequest request) {
        logger.debug("get");
        ensureOpen(request.index());
        return super.get(request);
    }

    @Override public void get(GetRequest request, ActionListener<GetResponse> listener) {
        logger.debug("get");
        ensureOpen(request.index());
        super.get(request, listener);
    }

    @Override public ActionFuture<CountResponse> count(CountRequest request) {
        logger.debug("count");
        ensureOpen(request.indices());
        return super.count(request);
    }

    @Override public void count(CountRequest request, ActionListener<CountResponse> listener) {
        logger.debug("count");
        ensureOpen(request.indices());
        super.count(request, listener);
    }

    @Override public ActionFuture<SearchResponse> search(SearchRequest request) {
        logger.debug("search");
        ensureOpen(request.indices());
        return super.search(request);
    }

    @Override public void search(SearchRequest request, ActionListener<SearchResponse> listener) {
        logger.debug("search");
        ensureOpen(request.indices());
        super.search(request, listener);
    }

    @Override public ActionFuture<SearchResponse> searchScroll(SearchScrollRequest request) {
        logger.debug("searchScroll");
        return super.searchScroll(request);
    }

    @Override public void searchScroll(SearchScrollRequest request, ActionListener<SearchResponse> listener) {
        logger.debug("searchScroll");
        super.searchScroll(request, listener);
    }

    @Override public ActionFuture<SearchResponse> moreLikeThis(MoreLikeThisRequest request) {
        logger.debug("moreLikeThis");
        ensureOpen(request.index());
        return super.moreLikeThis(request);
    }

    @Override public void moreLikeThis(MoreLikeThisRequest request, ActionListener<SearchResponse> listener) {
        logger.debug("moreLikeThis");
        ensureOpen(request.index());
        super.moreLikeThis(request, listener);
    }
    
}
