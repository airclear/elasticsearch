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
import org.elasticsearch.common.blobstore.ImmutableBlobContainer;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

import javax.annotation.Nullable;
import java.util.concurrent.Executor;

/**
 * @author Tom May (tom@gist.com)
 */
public class CassandraBlobStore extends AbstractComponent implements BlobStore {

    private final ESLogger logger = Loggers.getLogger(getClass());

    private final Executor executor;

    private final int bufferSizeInBytes; // XXX

    // XXX executor is a java.util.concurrent.ThreadPoolExecutor
    public CassandraBlobStore(Settings settings, Executor executor) {
        super(settings);

        this.executor = executor;

        this.bufferSizeInBytes = (int) settings.getAsBytesSize("buffer_size", new ByteSizeValue(100, ByteSizeUnit.KB)).bytes();

        logger.debug("CassandraBlobStore executor={}, bufferSizeInBytes={}", executor, bufferSizeInBytes);
    }

    @Override public String toString() {
        return "cassandra"; // XXX
    }

    public Executor executor() {
        return executor;
    }

    /* XXX
    public int bufferSizeInBytes() {
        return bufferSizeInBytes;
    }
    */

    @Override public ImmutableBlobContainer immutableBlobContainer(BlobPath path) {
        return new CassandraImmutableBlobContainer(path, this);
    }

    @Override public void delete(BlobPath path) {
        logger.debug("TODO delete path={}", path);
        /* XXX TODO
        ObjectListing prevListing = null;
        while (true) {
            ObjectListing list;
            if (prevListing != null) {
                list = client.listNextBatchOfObjects(prevListing);
            } else {
                list = client.listObjects(bucket, path.buildAsString("/"));
            }
            for (S3ObjectSummary summary : list.getObjectSummaries()) {
                client.deleteObject(summary.getBucketName(), summary.getKey());
            }
            if (list.isTruncated()) {
                prevListing = list;
            } else {
                break;
            }
        }
        */
    }

    @Override public void close() {
    }
}
