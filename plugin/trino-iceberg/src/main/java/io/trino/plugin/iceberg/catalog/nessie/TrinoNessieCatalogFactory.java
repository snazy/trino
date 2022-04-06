/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.iceberg.catalog.nessie;

import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.spi.security.ConnectorIdentity;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class TrinoNessieCatalogFactory
        implements TrinoCatalogFactory
{
    private final IcebergTableOperationsProvider tableOperationsProvider;
    private final String warehouseLocation;
    private final NessieIcebergClient nessieClient;
    private final boolean isUniqueTableLocation;
    private final String trinoVersion;

    @Inject
    public TrinoNessieCatalogFactory(
            IcebergTableOperationsProvider tableOperationsProvider,
            NessieIcebergClient nessieClient,
            NodeVersion nodeVersion,
            IcebergConfig icebergConfig)
    {
        this.tableOperationsProvider = requireNonNull(tableOperationsProvider, "tableOperationsProvider is null");
        this.trinoVersion = requireNonNull(nodeVersion, "nodeVersion is null").toString();
        this.nessieClient = requireNonNull(nessieClient, "nessieClient is null");
        this.warehouseLocation = nessieClient.getConfig().getDefaultWarehouseDir();
        requireNonNull(icebergConfig, "icebergConfig is null");
        this.isUniqueTableLocation = icebergConfig.isUniqueTableLocation();
    }

    @Override
    public TrinoCatalog create(ConnectorIdentity identity)
    {
        return new TrinoNessieCatalog(tableOperationsProvider, nessieClient, warehouseLocation, trinoVersion, isUniqueTableLocation);
    }
}
