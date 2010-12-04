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

package org.elasticsearch.client.node.lru;

import java.util.HashSet;
import java.util.Set;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
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
import org.elasticsearch.threadpool.ThreadPool;

public class LruNodeClient extends NodeClient {
    
    private final ESLogger logger = Loggers.getLogger(getClass());
    private final LruIndexManager lru;

    @Inject public LruNodeClient(Settings settings, ThreadPool threadPool, NodeAdminClient admin,
                              TransportIndexAction indexAction, TransportDeleteAction deleteAction, TransportBulkAction bulkAction,
                              TransportDeleteByQueryAction deleteByQueryAction, TransportGetAction getAction, TransportCountAction countAction,
                              TransportSearchAction searchAction, TransportSearchScrollAction searchScrollAction,
                              TransportMoreLikeThisAction moreLikeThisAction) {
        super(settings,threadPool,admin,indexAction,deleteAction,bulkAction,deleteByQueryAction,getAction,countAction,searchAction,searchScrollAction,moreLikeThisAction);
        this.lru = new LruIndexManager(settings.getAsInt("node.lrucache.size", 1), admin);
    }


    @Override public void index(final IndexRequest request, final ActionListener<IndexResponse> listener) {
        logger.debug("index");
        lru.ensureOpen(request.index(),new LruIndexManager.IndexOperation() {
            public void perform() {
                LruNodeClient.super.index(request,listener);
            }
        });
            
    }

    @Override public ActionFuture<DeleteResponse> delete(final DeleteRequest request) {
        logger.debug("delete");
        return (ActionFuture<DeleteResponse>)lru.ensureOpen(request.index(), new LruIndexManager.AsyncIndexOperation() {
            public ActionFuture<DeleteResponse> perform() {
                return LruNodeClient.super.delete(request);
            }
        });
    }

    @Override public void delete(final DeleteRequest request, final ActionListener<DeleteResponse> listener) {
        logger.debug("delete");
        lru.ensureOpen(request.index(),new LruIndexManager.IndexOperation() {
            public void perform() {
                LruNodeClient.super.delete(request, listener);
            }
        });
    }

    @Override public ActionFuture<BulkResponse> bulk(final BulkRequest request) {
        logger.debug("bulk");
        return (ActionFuture<BulkResponse>)lru.ensureOpen(getIndices(request), new LruIndexManager.AsyncIndexOperation() {
            public ActionFuture<BulkResponse> perform() {
                return LruNodeClient.super.bulk(request);
            }
        });
        
    }

    @Override public void bulk(final BulkRequest request, final ActionListener<BulkResponse> listener) {
        logger.debug("bulk");
        lru.ensureOpen(getIndices(request),new LruIndexManager.IndexOperation() {
            public void perform() {
                LruNodeClient.super.bulk(request,listener);
            }
        });
        
    }

    @Override public ActionFuture<DeleteByQueryResponse> deleteByQuery(final DeleteByQueryRequest request) {
        logger.debug("deleteByQuery");
        return (ActionFuture<DeleteByQueryResponse>)lru.ensureOpen(request.indices(), new LruIndexManager.AsyncIndexOperation() {
            public ActionFuture<DeleteByQueryResponse> perform() {
                return LruNodeClient.super.deleteByQuery(request);
            }
        });
    }

    @Override public void deleteByQuery(final DeleteByQueryRequest request, final ActionListener<DeleteByQueryResponse> listener) {
        logger.debug("deleteByQuery");
        lru.ensureOpen(request.indices(),new LruIndexManager.IndexOperation() {
            public void perform() {
                LruNodeClient.super.deleteByQuery(request,listener);
            }
        });
    }

    @Override public ActionFuture<GetResponse> get(final GetRequest request) {
        logger.debug("get");
        return (ActionFuture<GetResponse>)lru.ensureOpen(request.index(), new LruIndexManager.AsyncIndexOperation() {
            public ActionFuture<GetResponse> perform() {
                return LruNodeClient.super.get(request);
            }
        });
    }

    @Override public void get(final GetRequest request, final ActionListener<GetResponse> listener) {
        logger.debug("get");
        lru.ensureOpen(request.index(),new LruIndexManager.IndexOperation() {
            public void perform() {
                LruNodeClient.super.get(request, listener);
            }
        });
        
    }

    @Override public ActionFuture<CountResponse> count(final CountRequest request) {
        logger.debug("count");
        return (ActionFuture<CountResponse>)lru.ensureOpen(request.indices(), new LruIndexManager.AsyncIndexOperation() {
            public ActionFuture<CountResponse> perform() {
                return LruNodeClient.super.count(request);
            }
        });
    }

    @Override public void count(final CountRequest request, final ActionListener<CountResponse> listener) {
        logger.debug("count");
        lru.ensureOpen(request.indices(),new LruIndexManager.IndexOperation() {
            public void perform() {
                LruNodeClient.super.count(request, listener);
            }
        });
        
    }

    @Override public ActionFuture<SearchResponse> search(final SearchRequest request) {
        logger.debug("search");
        return (ActionFuture<SearchResponse>)lru.ensureOpen(request.indices(), new LruIndexManager.AsyncIndexOperation() {
            public ActionFuture<SearchResponse> perform() {
                return LruNodeClient.super.search(request);
            }
        });
    }

    @Override public void search(final SearchRequest request, final ActionListener<SearchResponse> listener) {
        logger.debug("search");
       lru.ensureOpen(request.indices(),new LruIndexManager.IndexOperation() {
            public void perform() {
                LruNodeClient.super.search(request, listener);
            }
        });
    }

    @Override public ActionFuture<SearchResponse> searchScroll(SearchScrollRequest request) {
        logger.debug("searchScroll");
        return super.searchScroll(request);
    }

    @Override public void searchScroll(SearchScrollRequest request, ActionListener<SearchResponse> listener) {
        logger.debug("searchScroll");
        super.searchScroll(request, listener);
    }

    @Override public ActionFuture<SearchResponse> moreLikeThis(final MoreLikeThisRequest request) {
        logger.debug("moreLikeThis");
        return (ActionFuture<SearchResponse>)lru.ensureOpen(request.index(), new LruIndexManager.AsyncIndexOperation() {
            public ActionFuture<SearchResponse> perform() {
                return LruNodeClient.super.moreLikeThis(request);
            }
        });
    }

    @Override public void moreLikeThis(final MoreLikeThisRequest request, final ActionListener<SearchResponse> listener) {
        logger.debug("moreLikeThis");
               lru.ensureOpen(request.index(),new LruIndexManager.IndexOperation() {
            public void perform() {
                LruNodeClient.super.moreLikeThis(request, listener);
            }
        });
    }

    private String [] getIndices(BulkRequest request) {
        Set<String> indices = new HashSet<String>();
        for(ActionRequest r : request.requests) {
            if(r instanceof IndexRequest) {
                indices.add(((IndexRequest)r).index());
            }
            else if(r instanceof DeleteRequest) {
                indices.add(((DeleteRequest)r).index());
            }
        }
        return indices.toArray(new String[indices.size()]);
    }


}
