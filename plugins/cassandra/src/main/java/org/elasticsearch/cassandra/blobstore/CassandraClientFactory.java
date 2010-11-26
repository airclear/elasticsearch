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

import java.io.IOException;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import org.apache.cassandra.thrift.Cassandra;

class CassandraClientFactory {
    private final String host;
    private final int port;

    public CassandraClientFactory(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public Cassandra.Client getCassandraClient()
        throws IOException
    {
        TTransport transport =
            new TFramedTransport(new TSocket(host, port));
        TProtocol protocol = new TBinaryProtocol(transport);
        Cassandra.Client client = new Cassandra.Client(protocol);
        try {
            transport.open();
        }
        catch (TTransportException ex) {
            throw new IOException(
                "Cassandra transport.open to " + host + ":" + port + " failed",
                ex);
        }
        return client;
    }

    public void closeCassandraClient(Cassandra.Client client) {
        client.getInputProtocol().getTransport().close();
    }
}