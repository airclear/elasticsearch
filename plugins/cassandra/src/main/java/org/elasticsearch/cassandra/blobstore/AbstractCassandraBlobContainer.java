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
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;

import org.apache.thrift.TException;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.List;

/**
 * @author Tom May (tom@gist.com)
 */
public class AbstractCassandraBlobContainer extends AbstractBlobContainer {

    protected static final Charset utf8 = Charset.forName("UTF-8");

    protected static final String keySpace = "ElasticSearch";

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
        try {
            logger.debug("TODO blobExists blobName={}", blobName);
            Cassandra.Client client =
                CassandraClientFactory.getCassandraClient();
            try {
                return client.get_count(
                    keySpace,
                    blobKey(blobName),
                    new ColumnParent("Blobs"),
                    ConsistencyLevel.QUORUM) != 0;
            }
            finally {
                CassandraClientFactory.closeCassandraClient(client);
            }
        } catch (Exception e) {
            return false;
        }
    }

    @Override public boolean deleteBlob(String blobName) throws IOException {
        logger.debug("TODO deleteBlob blobName={}", blobName);
        return true;
    }

    @Override public void readBlob(final String blobName, final ReadBlobListener listener) {
        logger.debug("readBlob blobName={}", blobName);
        blobStore.executor().execute(new Runnable() {
            @Override public void run() {
                Cassandra.Client client = null;
                try {
                    client = CassandraClientFactory.getCassandraClient();
                    readBlob(client, blobName, listener);
                }
                catch (Exception ex) {
                    listener.onFailure(ex);
                }
                finally {
                    if (client != null) {
                        CassandraClientFactory.closeCassandraClient(client);
                    }
                }
            }
        });
    }

    private void readBlob(Cassandra.Client client, String blobName, ReadBlobListener listener)
        throws Exception
    {
        ColumnOrSuperColumn columnOrSuperColumn = client.get(
            keySpace,
            blobKey(blobName),
            new ColumnPath("Blobs").setColumn(utf8.encode("data")),
            ConsistencyLevel.QUORUM);
        Column column = columnOrSuperColumn.getColumn();
        byte[] blobData = column.getValue();
        logger.debug("Read {} ({} bytes): {}",
            blobName, blobData.length, new String(blobData));
        listener.onPartial(blobData, 0, blobData.length);
        listener.onCompleted();
    }

    @Override public ImmutableMap<String, BlobMetaData> listBlobsByPrefix(@Nullable String blobNamePrefix) throws IOException {
        logger.debug("listBlobsByPrefix blobNamePrefix={}", blobNamePrefix);

        List<ColumnOrSuperColumn> columns;
        Cassandra.Client client = CassandraClientFactory.getCassandraClient();
        try {
            columns =
                client.get_slice(
                    keySpace,
                    blobPath,
                    new ColumnParent("BlobNames"),
                    new SlicePredicate().setSlice_range(
                        new SliceRange()
                        .setStart(new byte[0])
                        .setFinish(new byte[0])
                        .setCount(1000000000)),
                    ConsistencyLevel.QUORUM);
        }
        catch (InvalidRequestException ex) {
            throw new IOException("Cassandra get_slice on ???:??? failed", ex);
        }
        catch (UnavailableException ex) {
            throw new IOException("Cassandra get_slice on ???:??? failed", ex);
        }
        catch (TimedOutException ex) {
            throw new IOException("Cassandra get_slice on ???:??? failed", ex);
        }
        catch (TException ex) {
            throw new IOException("Cassandra get_slice on ???:??? failed", ex);
        }
        finally {
            CassandraClientFactory.closeCassandraClient(client);
        }

        ImmutableMap.Builder<String, BlobMetaData> blobsBuilder = ImmutableMap.builder();

        for (ColumnOrSuperColumn columnOrSuperColumn : columns) {
            Column column = columnOrSuperColumn.getColumn();
            String name = new String(column.getName(), utf8);
            long length = Integer.parseInt(new String(column.getValue(), utf8));
            logger.debug("name: {}, length: {}", name, length);
            if (blobNamePrefix == null || name.startsWith(blobNamePrefix)) {
                blobsBuilder.put(name, new PlainBlobMetaData(name, length));
            }
        }

        return blobsBuilder.build();
    }

    @Override public ImmutableMap<String, BlobMetaData> listBlobs() throws IOException {
        return listBlobsByPrefix(null);
    }

    protected String blobKey(String blobName) {
        return blobPath + '/' + blobName;
    }
}
