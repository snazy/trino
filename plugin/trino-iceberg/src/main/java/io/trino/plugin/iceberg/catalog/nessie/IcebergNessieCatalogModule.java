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

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.http.HttpClientBuilder;

import javax.inject.Provider;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class IcebergNessieCatalogModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(NessieConfig.class);
        binder.bind(NessieIcebergClient.class).toProvider(NessieIcebergClientProvider.class).in(Scopes.SINGLETON);
        binder.bind(NessieApiV1.class).toProvider(NessieApiProvider.class).in(Scopes.SINGLETON);
        binder.bind(IcebergTableOperationsProvider.class).to(NessieIcebergTableOperationsProvider.class).in(Scopes.SINGLETON);
        newExporter(binder).export(IcebergTableOperationsProvider.class).withGeneratedName();
        binder.bind(TrinoCatalogFactory.class).to(TrinoNessieCatalogFactory.class).in(Scopes.SINGLETON);
        newExporter(binder).export(TrinoCatalogFactory.class).withGeneratedName();
    }

    public static class NessieApiProvider
            implements Provider<NessieApiV1>
    {
        private final NessieConfig nessieConfig;

        @Inject
        public NessieApiProvider(NessieConfig nessieConfig)
        {
            this.nessieConfig = nessieConfig;
        }

        @Override
        public NessieApiV1 get()
        {
            return HttpClientBuilder.builder()
                    .withUri(nessieConfig.getServerUri())
                    .build(NessieApiV1.class);
        }
    }

    public static class NessieIcebergClientProvider
            implements Provider<NessieIcebergClient>
    {
        private final NessieApiV1 nessieApi;
        private final NessieConfig nessieConfig;

        @Inject
        public NessieIcebergClientProvider(NessieApiV1 nessieApi, NessieConfig nessieConfig)
        {
            this.nessieApi = nessieApi;
            this.nessieConfig = nessieConfig;
        }

        @Override
        public NessieIcebergClient get()
        {
            return new NessieIcebergClient(nessieApi, nessieConfig);
        }
    }
}
