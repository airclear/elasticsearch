package org.elasticsearch.plugin.lruclient;

import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.status.ShardStatus;
import org.elasticsearch.client.node.NodeAdminClient;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.indices.IndexMissingException;

/**
 *
 * @author matt
 */
public class LruIndexManager {
    public static final int MAX_OPEN_RETRIES = 3;

    private Map cache;
    private NodeAdminClient admin;
    private final ESLogger logger = Loggers.getLogger(getClass());

    public LruIndexManager(int cache_size, NodeAdminClient admin) {
        this.cache = Collections.synchronizedMap(new LruCache(cache_size));
        this.admin = admin;
    }

    public interface IndexOperation {
        public void perform();
    }

    public interface AsyncIndexOperation {
        public ActionFuture<? extends ActionResponse> perform();
    }

    
    public ActionFuture<? extends ActionResponse> ensureOpen(final String index, AsyncIndexOperation op) {
        return ensureOpenAndPerform(new String[]{index},op);
    }

    public ActionFuture<? extends ActionResponse> ensureOpen(final String [] indices, AsyncIndexOperation op) {
        return ensureOpenAndPerform(indices,op);
    }

    public void ensureOpen(final String index, IndexOperation op) {
        ensureOpenAndPerform(new String[]{index},op);
    }

    public void ensureOpen(final String [] indices, IndexOperation op) {
        ensureOpenAndPerform(indices,op);
    }

    // private

    private ActionFuture<? extends ActionResponse> ensureOpenAndPerform(String [] indices, Object op) {
        int tries = 0;
        boolean force = false;
        while(true) {

            // make sure in cache
            for(int i = 0; i<indices.length; i++)
                openIfNotCached(indices[i],force);

            try {
                if(op instanceof AsyncIndexOperation)
                    return ((AsyncIndexOperation)op).perform();
                else {
                    ((IndexOperation)op).perform();
                    return null;
                }

            }
            catch(IndexMissingException e) {
                if(tries++>MAX_OPEN_RETRIES)
                    throw(e);
                else
                    force = true;  // make sure we eject the index
            }
        }
    }

    private void openIfNotCached(final String index, boolean force_eject) {
        synchronized(index.intern()) {
            if(force_eject)
              this.cache.remove(index);

            if(!this.cache.containsKey(index)) {
                logger.debug("Index {} not found!  Try to open...", index);
                openIndex(index);
            }
            this.cache.put(index, new Date());
        }
    }
    
    private class LruCache extends LinkedHashMap<String, Date> {
        private int capacity;
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

    private void openIndex(final String index) {
        try {
            admin.indices().prepareOpen(index).execute().actionGet();
            waitForReadyState(index);
        }
        catch(IndexMissingException e) {
            logger.debug("Index {} does not exist!  Can't open.", index);
        }
    }

    private void waitForReadyState(final String index) {
        boolean notReady = true;
        long stop=System.nanoTime()+TimeUnit.SECONDS.toNanos(10);
        while(notReady) {
            if(stop<System.nanoTime()) {
                logger.debug("Timeout waiting for status of index {}",index);
                break;
            }
            // need to wait till all shards are allocated!
            logger.debug("Checking for state in index {}",index);
            ShardStatus [] stats = admin.indices().prepareStatus(new String[]{index}).execute().actionGet().getShards();
            logger.debug("Checking for state in index {} with # shards: {}",index,stats.length);
            if(stats.length<1) {
                logger.debug("No shards found for index {}!!",index);
                try { Thread.sleep(500); } catch(InterruptedException e) {}
                continue;
            }
            notReady = false;
            for(int i=0;i<stats.length;i++) {
                logger.debug("Index {} Shard {} State {}",index,stats[i].getShardId(),stats[i].getState());
                if(stats[i].getState() != IndexShardState.STARTED) {
                    try { Thread.sleep(500); } catch(InterruptedException e) {}
                    notReady = true;
                    break;
                }
            }
        }
        logger.debug("Index {} all shards STARTED!",index);
    }

    private void closeIndex(final String index) {
        // let happen in the background
        logger.debug("Closing index: {} ...", index);
        admin.indices().prepareClose(index).execute().actionGet();
    }


}
