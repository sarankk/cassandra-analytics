/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.bridge;

import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.SchemaProvider;
import org.apache.cassandra.schema.SchemaTransformation;
import org.apache.cassandra.schema.SchemaTransformations;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.tcm.ClusterMetadata;

/**
 * Cassandra 6.0 replaced {@code Schema.transform} with {@code SchemaProvider.submit}, which routes the
 * transformation through Transactional Cluster Metadata (CEP-21) and returns the resulting ClusterMetadata.
 *
 * <p>On a real node a {@code SchemaListener} registered with the metadata log reacts to each commit by
 * instantiating the {@code Keyspace} and {@code ColumnFamilyStore} objects for the new schema. The offline
 * metadata service the bridge installs has no listeners, and {@code Schema.getKeyspaceInstance} now resolves
 * instances through {@code ClusterMetadata.current().schema}, so without that step the schema would exist as
 * metadata while every keyspace instance stayed null. We therefore do what the listener would have done,
 * with {@code loadSSTables} false because analytics only ever writes new SSTables.
 */
public class SchemaUpdater
{
    private SchemaUpdater()
    {
    }

    public static void load(SchemaProvider schema, KeyspaceMetadata keyspaceMetadata)
    {
        submit(schema, SchemaTransformations.addKeyspace(keyspaceMetadata, false));
    }

    public static void load(SchemaProvider schema, KeyspaceMetadata keyspaceMetadata, TableMetadata tableMetadata)
    {
        submit(schema, SchemaTransformations.addTable(tableMetadata, false));
    }

    public static void load(SchemaProvider schema, KeyspaceMetadata keyspaceMetadata, Types userTypes)
    {
        submit(schema, SchemaTransformations.addTypes(userTypes, true));
    }

    public static void updateTable(SchemaProvider schema, KeyspaceMetadata keyspaceMetadata, TableMetadata tableMetadata)
    {
        // SchemaTransformation gained a second abstract method (compatibleWith) in 6.0, so it is no longer
        // a functional interface and the transformation has to be spelled out. It is also handed the whole
        // ClusterMetadata now rather than just the Keyspaces it should transform.
        submit(schema, new SchemaTransformation()
        {
            @Override
            public Keyspaces apply(ClusterMetadata metadata)
            {
                return metadata.schema.getKeyspaces()
                                      .withAddedOrUpdated(keyspaceMetadata.withSwapped(keyspaceMetadata.tables.withSwapped(tableMetadata)));
            }

            @Override
            public boolean compatibleWith(ClusterMetadata metadata)
            {
                return true;
            }
        });
    }

    private static void submit(SchemaProvider schema, SchemaTransformation transformation)
    {
        DistributedSchema before = ClusterMetadata.current().schema;
        ClusterMetadata after = schema.submit(transformation);
        after.schema.initializeKeyspaceInstances(before, false);
    }
}
