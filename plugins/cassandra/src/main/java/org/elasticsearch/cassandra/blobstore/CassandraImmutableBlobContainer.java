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
import org.elasticsearch.common.blobstore.ImmutableBlobContainer;
import org.elasticsearch.common.blobstore.support.BlobStores;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Tom May (tom@gist.com)
 */
public class CassandraImmutableBlobContainer extends AbstractCassandraBlobContainer implements ImmutableBlobContainer {

    public CassandraImmutableBlobContainer(BlobPath path, CassandraBlobStore blobStore) {
        super(path, blobStore);
    }

    // InputStream is a completely shitty abstraction for something to
    // write via thrift.  And passing a sizeInBytes along with an
    // InputStream is a sign that it's a shitty abstraction in
    // general.  At least we can use the sizeInBytes to allocate a
    // ByteBuffer and copy to it then hand it to thrift.
    @Override public void writeBlob(final String blobName, final InputStream is, final long sizeInBytes, final WriterListener listener) {
        blobStore.executor().execute(new Runnable() {
            @Override public void run() {
                try {
                    logger.debug("writeBlob blobKey={}, sizeInBytes={}", blobKey(blobName), sizeInBytes);
                    Cassandra.Client client =
                        CassandraClientFactory.getCassandraClient();
                    try {
                        writeBlob(client, blobName, is, sizeInBytes);
                    }
                    finally {
                        CassandraClientFactory.closeCassandraClient(client);
                    }
                    listener.onCompleted();
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }
        });
    }

    @Override public void writeBlob(String blobName, InputStream is, long sizeInBytes) throws IOException {
        BlobStores.syncWriteBlob(this, blobName, is, sizeInBytes);
    }

    private void writeBlob(Cassandra.Client client, String blobName, InputStream is, long sizeInBytes)
        throws InvalidRequestException, TimedOutException, UnavailableException, TException, IOException
    {
        long timestamp = System.currentTimeMillis();

        Map<String, Map<String, List<Mutation>>> mutationMap =
            new HashMap<String, Map<String, List<Mutation>>>();

        // Insert the blob data into Blobs.

        int intSizeInBytes = (int) sizeInBytes;
        if (intSizeInBytes != sizeInBytes) {
            throw new IllegalArgumentException(
                "Blob " + blobName + " size " + sizeInBytes +
                " is too large.");
        }
        ByteBuffer blobData = ByteBuffer.allocate(intSizeInBytes);
        new DataInputStream(is).readFully(blobData.array());

        List<Mutation> blobsMutations = new ArrayList<Mutation>();
        blobsMutations.add(createInsert("data", blobData, timestamp));

        Map<String, List<Mutation>> blobsMutationMap =
            new HashMap<String, List<Mutation>>();
        blobsMutationMap.put("Blobs", blobsMutations);

        mutationMap.put(blobKey(blobName), blobsMutationMap);

        // Insert the blobName into BlobNames.

        ByteBuffer size = utf8.encode(Long.toString(sizeInBytes));

        List<Mutation> blobNamesMutations = new ArrayList<Mutation>();
        blobNamesMutations.add(createInsert(blobName, size, timestamp));

        Map<String, List<Mutation>> blobNamesMutationMap =
            new HashMap<String, List<Mutation>>();
        blobNamesMutationMap.put("BlobNames", blobNamesMutations);

        mutationMap.put(blobPath, blobNamesMutationMap);

        client.batch_mutate(
            keySpace, mutationMap, ConsistencyLevel.QUORUM);
    }

    private Mutation createInsert(String name, ByteBuffer value, long timestamp) {
        return new Mutation().setColumn_or_supercolumn(
            new ColumnOrSuperColumn().setColumn(
                new Column(
                    utf8.encode(name),
                    value,
                    timestamp)));
    }
}
