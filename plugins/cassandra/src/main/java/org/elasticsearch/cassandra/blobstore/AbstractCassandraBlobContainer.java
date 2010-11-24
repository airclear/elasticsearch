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

import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * @author Tom May (tom@gist.com)
 */
public class AbstractCassandraBlobContainer extends AbstractBlobContainer {
    protected final ESLogger logger = Loggers.getLogger(getClass());

    protected final String blobPath;

    protected final CassandraBlobStore blobStore;

    public AbstractCassandraBlobContainer(BlobPath path, CassandraBlobStore blobStore) {
        super(path);
        this.blobPath = path.buildAsString("/");
        this.blobStore = blobStore;
        logger.debug("AbstractCassandraBlobContainer path={}", path);
    }

    @Override public boolean blobExists(String blobName) {
        return blobStore.blobExists(blobPath, blobName);
    }

    @Override public boolean deleteBlob(String blobName) throws IOException {
        return blobStore.deleteBlob(blobPath, blobName);
    }

    @Override public void readBlob(String blobName, ReadBlobListener listener) {
        blobStore.readBlob(blobPath, blobName, listener);
    }

    @Override public ImmutableMap<String, BlobMetaData> listBlobsByPrefix(@Nullable String blobNamePrefix) throws IOException {
        return blobStore.listBlobsByPrefix(blobPath, blobNamePrefix);
    }

    @Override public ImmutableMap<String, BlobMetaData> listBlobs() throws IOException {
        return listBlobsByPrefix(null);
    }
}
