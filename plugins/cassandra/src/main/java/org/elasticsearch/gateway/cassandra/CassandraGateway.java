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

package org.elasticsearch.gateway.cassandra;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
//import org.elasticsearch.cloud.aws.AwsS3Service;
//import org.elasticsearch.cloud.aws.blobstore.S3BlobStore;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.MetaDataCreateIndexService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.gateway.blobstore.BlobStoreGateway;
import org.elasticsearch.index.gateway.cassandra.CassandraIndexGatewayModule;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;

/**
 * @author Tom May (tom@gist.com)
 */
public class CassandraGateway extends BlobStoreGateway {

    @Inject public CassandraGateway(Settings settings, ClusterService clusterService, MetaDataCreateIndexService createIndexService,
    ClusterName clusterName, ThreadPool threadPool /* XXX , AwsS3Service s3Service*/) throws IOException {
        super(settings, clusterService, createIndexService);
        
        /* XXX
        String bucket = componentSettings.get("bucket");
        if (bucket == null) {
            throw new ElasticSearchIllegalArgumentException("No bucket defined for s3 gateway");
        }

        String region = componentSettings.get("region");
        ByteSizeValue chunkSize = componentSettings.getAsBytesSize("chunk_size", new ByteSizeValue(100, ByteSizeUnit.MB));

        logger.debug("using bucket [{}], region [{}], chunk_size [{}]", bucket, region, chunkSize);

        initialize(new S3BlobStore(settings, s3Service.client(), bucket, region, threadPool.cached()), clusterName, chunkSize);
        */
    }

    @Override public void close() throws ElasticSearchException {
        super.close();
    }

    @Override public String type() {
        return "cassandra";
    }

    @Override public Class<? extends Module> suggestIndexGateway() {
        return CassandraIndexGatewayModule.class;
    }
}
