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

/**
 * The cassandra column families are defined in storage-conf.xml like
 * this:
 *    <!-- Key is BlobPath/BlobName.
 *         Column name is "data".
 *         Value is blob data. -->
 *    <ColumnFamily Name="Blobs"/>
 *                  />
 *    <!-- Key is BlobPath.
 *         Column names are BlobNames.
 *         Value is size as a string. -->
 *    <ColumnFamily Name="BlobNames"/>
 *
 * ElasticSearch needs us to support the following:
 * 1. Read, write, and delete a blob named by BlobPath + BlobName.
 * 2. Determine whether a Blob exists.  (Actually this method, blobExists,
 *    is not referenced in the code.)
 * 3. Get a list of BlobNames in a BlobPath, with their sizes.
 * 4. Delete all blobs in a BlobPath.
 *
 * Here are a couple ways to store the data, and why they don't work:
 * A. A single column family with BlobPath as the key and BlobName as the
 *    column name.  1 and 4 are easy, but getting the sizes for 3 isn't
 *    possible without fetching the entire blob.  Same with 2.
 * B. A single supercolumn family with BlobPath as a key and BlobName as
 *    the supercolumn name, with subcolumns data and size.  1, 2, and 4
 *    are easy, but fetching the size for 3 requires fetching the entire
 *    supercolumn, we can't just pick out the size column in a get_slice
 *    request.
 *
 * The storage layout used allows us to do everything we need even though
 * it's a bit more complicated than A and B because it has two column
 * families.
 * X. Storing the blob names and sizes in BlobNames makes 3 possible,
 *    but complicates 1 since we need to track things in BlobNames.
 * Y. Using BlobPath/BlobName as the key and storing the data in a column
 *    makes 2 possible using get_count.
 * Z. But it complicates 4, which must be done by fetching the BlobPath's
 *    BlobNames then deleting them.
 */

package org.elasticsearch.cassandra.blobstore;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.ImmutableBlobContainer;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;

import org.apache.thrift.TException;

import javax.annotation.Nullable;
import java.io.DataInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * @author Tom May (tom@gist.com)
 */
public class CassandraBlobStore extends AbstractComponent implements BlobStore {
    private static final String keySpace = "ElasticSearch";

    private static final Charset utf8 = Charset.forName("UTF-8");

    private final ESLogger logger = Loggers.getLogger(getClass());

    private final Executor executor;

    private final int bufferSizeInBytes; // XXX

    // XXX executor is a java.util.concurrent.ThreadPoolExecutor
    public CassandraBlobStore(Settings settings, Executor executor) {
        super(settings);

        this.executor = executor;

        this.bufferSizeInBytes = (int) settings.getAsBytesSize("buffer_size", new ByteSizeValue(100, ByteSizeUnit.KB)).bytes();

        logger.debug("CassandraBlobStore executor: {} bufferSizeInBytes: {}", executor, bufferSizeInBytes);
    }

    @Override public String toString() {
        return "cassandra"; // XXX
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
        String blobPath = path.buildAsString("/");
        logger.debug("TODO delete {}", blobPath);
        try {
            ImmutableMap<String, BlobMetaData> blobs =
                listBlobsByPrefix(blobPath, null);
            String[] blobNames =
                blobs.keySet().toArray(new String[blobs.size()]);
            deleteBlobs(blobPath, blobNames);
        }
        catch (IOException ex) {
            // Oh well, nothing we can do but log.
            logger.warn("delete {} failed", ex, blobPath);
        }
    }

    @Override public void close() {
    }

    boolean blobExists(String blobPath, String blobName) {
        String blobKey = blobKey(blobPath, blobName);
        logger.debug("TODO blobExists {}", blobKey);
        try {
            Cassandra.Client client =
                CassandraClientFactory.getCassandraClient();
            try {
                return client.get_count(
                    keySpace,
                    blobKey,
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

    boolean deleteBlob(String blobPath, String blobName) throws IOException {
        logger.debug("deleteBlob {}", blobKey(blobPath, blobName));
        return deleteBlobs(blobPath, blobName);
    }

    private boolean deleteBlobs(String blobPath, String... blobNames)
        throws IOException
    {
        long timestamp = System.currentTimeMillis();

        Map<String, Map<String, List<Mutation>>> mutationMap =
            Maps.newHashMap();

        // Delete the blob data from Blobs.

        for (String blobName : blobNames) {
            String blobKey = blobKey(blobPath, blobName);

            Map<String, List<Mutation>> blobsMutationMap = Maps.newHashMap();
            blobsMutationMap.put(
                "Blobs", 
                ImmutableList.of(createDelete(null, timestamp)));

            mutationMap.put(blobKey, blobsMutationMap);
        }

        // Delete the blobNames from BlobNames.

        List<Mutation> blobNamesMutations = Lists.newArrayList();
        for (String blobName : blobNames) {
            blobNamesMutations.add(createDelete(blobName, timestamp));
        }

        Map<String, List<Mutation>> blobNamesMutationMap =
            ImmutableMap.of("BlobNames", blobNamesMutations);

        mutationMap.put(blobPath, blobNamesMutationMap);

        Cassandra.Client client = null;
        try {
            client = CassandraClientFactory.getCassandraClient();
            client.batch_mutate(
                keySpace, mutationMap, ConsistencyLevel.QUORUM);
            return true;
        }
        catch (Exception e) {
            // TODO S3 does this, what's the deal with returning false
            // vs. throwing IOException?
            return false;
        }
        finally {
            if (client != null) {
                CassandraClientFactory.closeCassandraClient(client);
            }
        }
    }

    private Mutation createDelete(String name, long timestamp) {
        Deletion deletion = new Deletion(timestamp);
        if (name != null) {
            deletion.setPredicate(
                new SlicePredicate().setColumn_names(
                    ImmutableList.of(utf8.encode(name))));
        }
        return new Mutation().setDeletion(deletion);
    }

    void readBlob(String blobPath, String blobName, final BlobContainer.ReadBlobListener listener) {
        final String blobKey = blobKey(blobPath, blobName);
        logger.debug("readBlob {}", blobKey);
            executor.execute(new Runnable() {
            @Override public void run() {
                Cassandra.Client client = null;
                try {
                    client = CassandraClientFactory.getCassandraClient();
                    readBlob(client, blobKey, listener);
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

    private void readBlob(Cassandra.Client client, String blobKey, BlobContainer.ReadBlobListener listener)
        throws Exception
    {
        ColumnOrSuperColumn columnOrSuperColumn = client.get(
            keySpace,
            blobKey,
            new ColumnPath("Blobs").setColumn(utf8.encode("data")),
            ConsistencyLevel.QUORUM);
        Column column = columnOrSuperColumn.getColumn();
        byte[] blobData = column.getValue();
        logger.debug("Read {} bytes: {}", blobKey, blobData.length);
        listener.onPartial(blobData, 0, blobData.length);
        listener.onCompleted();
    }

    ImmutableMap<String, BlobMetaData> listBlobsByPrefix(String blobPath, @Nullable String blobNamePrefix) throws IOException {
        logger.debug("listBlobsByPrefix {}", blobKey(blobPath, blobNamePrefix));
        List<ColumnOrSuperColumn> columns;
        Cassandra.Client client = CassandraClientFactory.getCassandraClient();
        try {
            columns = client.get_slice(
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
            logger.debug("name: {} length: {}", name, length);
            if (blobNamePrefix == null || name.startsWith(blobNamePrefix)) {
                blobsBuilder.put(name, new PlainBlobMetaData(name, length));
            }
        }

        return blobsBuilder.build();
    }

    void writeBlob(final String blobPath, final String blobName, final InputStream is, final long sizeInBytes, final ImmutableBlobContainer.WriterListener listener) {
        logger.debug("writeBlob {} sizeInBytes: {}", blobKey(blobPath, blobName), sizeInBytes);
        executor.execute(new Runnable() {
            @Override public void run() {
                try {
                    Cassandra.Client client =
                        CassandraClientFactory.getCassandraClient();
                    try {
                        writeBlob(client, blobPath, blobName, is, sizeInBytes);
                        listener.onCompleted();
                    }
                    finally {
                        CassandraClientFactory.closeCassandraClient(client);
                    }
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }
        });
    }

    // InputStream is a completely shitty abstraction for something to
    // write via thrift.  And passing a sizeInBytes along with an
    // InputStream is a sign that it's a shitty abstraction in
    // general.  At least we can use the sizeInBytes to allocate a
    // ByteBuffer and copy to it then hand it to thrift.
    private void writeBlob(Cassandra.Client client, String blobPath, String blobName, InputStream is, long sizeInBytes)
        throws InvalidRequestException, TimedOutException, UnavailableException, TException, IOException
    {
        String blobKey = blobKey(blobPath, blobName);

        long timestamp = System.currentTimeMillis();

        Map<String, Map<String, List<Mutation>>> mutationMap =
            Maps.newHashMap();

        // Insert the blob data into Blobs.

        int intSizeInBytes = (int) sizeInBytes;
        if (intSizeInBytes != sizeInBytes) {
            throw new IllegalArgumentException(
                "Blob " + blobKey + " size " + sizeInBytes +
                " is too large.");
        }
        ByteBuffer blobData = ByteBuffer.allocate(intSizeInBytes);
        new DataInputStream(is).readFully(blobData.array());

        Map<String, List<Mutation>> blobsMutationMap = Maps.newHashMap();
        blobsMutationMap.put(
            "Blobs",
            ImmutableList.of(createInsert("data", blobData, timestamp)));

        mutationMap.put(blobKey, blobsMutationMap);

        // Insert the blobName into BlobNames.

        ByteBuffer size = utf8.encode(Long.toString(sizeInBytes));

        Map<String, List<Mutation>> blobNamesMutationMap =
            Maps.newHashMap();
        blobNamesMutationMap.put(
            "BlobNames",
            ImmutableList.of(createInsert(blobName, size, timestamp)));

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

    private String blobKey(String blobPath, String blobName) {
        return blobPath + '/' + blobName;
    }
}
