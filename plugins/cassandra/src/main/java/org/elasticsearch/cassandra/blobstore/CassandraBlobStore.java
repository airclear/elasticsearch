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

package org.elasticsearch.cassandra.blobstore;

import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.util.concurrent.Executor;

/**
 * @author Tom May (tom@gist.com)
 */
public class CassandraBlobStore extends AbstractComponent implements BlobStore {
    private final ESLogger logger = Loggers.getLogger(getClass());

    private final String keyspace;

    private final String hostAndPort;

    private final CassandraClientFactory cassandraClientFactory;

    private final Executor executor;

    private final int bufferSizeInBytes; // XXX

    // XXX executor is a java.util.concurrent.ThreadPoolExecutor
    public CassandraBlobStore(Settings settings, Executor executor) {
        super(settings);

        keyspace = settings.get("keyspace", "ElasticSearch");

        String host = settings.get("host", "localhost");
        int port = settings.getAsInt("port", 9160);
        hostAndPort = host + ':' + port;
        cassandraClientFactory = new CassandraClientFactory(host, port);

        this.executor = executor;

        this.bufferSizeInBytes = (int) settings.getAsBytesSize("buffer_size", new ByteSizeValue(100, ByteSizeUnit.KB)).bytes();

        logger.debug("CassandraBlobStore {} executor: {} bufferSizeInBytes: {}",
            this, executor, bufferSizeInBytes);
    }

    @Override public String toString() {
        return hostAndPort;
    }

    /* XXX
    public int bufferSizeInBytes() {
        return bufferSizeInBytes;
    }
    */

    @Override public CassandraBlobContainer immutableBlobContainer(BlobPath path) {
        return new CassandraBlobContainer(path, keyspace, cassandraClientFactory, executor);
    }

    @Override public void delete(BlobPath path) {
        immutableBlobContainer(path).delete();
    }

    @Override public void close() {
    }
}
