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

package org.elasticsearch.plugin.cassandra;

import org.elasticsearch.plugins.AbstractPlugin;

// Called at startup.  We can get the Setting as a constructor
// argument, set up injector modules, etc., here if we need to.
// See for example
// plugins/cloud/aws/src/main/java/org/elasticsearch/plugin/cloud/aws/CloudAwsPlugin.java

/**
 * @author matt.hartzler
 * @author Tom May (tom@gist.com)
 */
public class CassandraGatewayPlugin extends AbstractPlugin {

    @Override public String name() {
        return "cassandra-gateway";
    }

    @Override public String description() {
        return "Cassandra Gateway";
    }
}
