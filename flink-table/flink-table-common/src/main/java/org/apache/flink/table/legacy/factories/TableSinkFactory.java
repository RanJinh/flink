/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.legacy.factories;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.expressions.DefaultSqlFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.legacy.sinks.TableSink;

import java.util.Map;

/**
 * A factory to create configured table sink instances in a batch or stream environment based on
 * string-based properties. See also {@link TableFactory} for more information.
 *
 * @param <T> type of records that the factory produces
 * @deprecated This interface has been replaced by {@link DynamicTableSinkFactory}. The new
 *     interface consumes internal data structures. See FLIP-95 for more information.
 */
@Deprecated
@Internal
public interface TableSinkFactory<T> extends TableFactory {

    /**
     * Creates and configures a {@link TableSink} using the given properties.
     *
     * @param properties normalized properties describing a table sink.
     * @return the configured table sink.
     * @deprecated {@link Context} contains more information, and already contains table schema too.
     *     Please use {@link #createTableSink(Context)} instead.
     */
    @Deprecated
    default TableSink<T> createTableSink(Map<String, String> properties) {
        return null;
    }

    /**
     * Creates and configures a {@link TableSink} based on the given {@link CatalogTable} instance.
     *
     * @param tablePath path of the given {@link CatalogTable}
     * @param table {@link CatalogTable} instance.
     * @return the configured table sink.
     * @deprecated {@link Context} contains more information, and already contains table schema too.
     *     Please use {@link #createTableSink(Context)} instead.
     */
    @Deprecated
    default TableSink<T> createTableSink(ObjectPath tablePath, CatalogTable table) {
        return createTableSink(
                ((ResolvedCatalogTable) table).toProperties(DefaultSqlFactory.INSTANCE));
    }

    /**
     * Creates and configures a {@link TableSink} based on the given {@link Context}.
     *
     * @param context context of this table sink.
     * @return the configured table sink.
     */
    default TableSink<T> createTableSink(Context context) {
        return createTableSink(context.getObjectIdentifier().toObjectPath(), context.getTable());
    }

    /** Context of table sink creation. Contains table information and environment information. */
    @Internal
    interface Context {

        /**
         * @return full identifier of the given {@link CatalogTable}.
         */
        ObjectIdentifier getObjectIdentifier();

        /**
         * @return table {@link CatalogTable} instance.
         */
        CatalogTable getTable();

        /**
         * @return readable config of this table environment. The configuration gives the ability to
         *     access {@code TableConfig#getConfiguration()} which holds the current {@code
         *     TableEnvironment} session configurations.
         */
        ReadableConfig getConfiguration();

        /**
         * It depends on whether the {@code TableEnvironment} execution mode is batch.
         *
         * <p>In the future, the new sink interface will infer from input to source. See {@link
         * Source#getBoundedness}.
         */
        boolean isBounded();

        /** Whether the table is temporary. */
        boolean isTemporary();
    }
}
