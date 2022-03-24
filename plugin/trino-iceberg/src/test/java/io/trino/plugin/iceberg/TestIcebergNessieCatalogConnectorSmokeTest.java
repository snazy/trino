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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.QueryRunner;
import io.trino.testing.containers.NessieContainer;
import org.apache.iceberg.FileFormat;
import org.testcontainers.containers.Network;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testcontainers.containers.Network.newNetwork;

public class TestIcebergNessieCatalogConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    private static NessieContainer nessieContainer;
    private static Network network;
    private static Path tempDir;

    public TestIcebergNessieCatalogConnectorSmokeTest()
    {
        super(FileFormat.PARQUET);
    }

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        network = newNetwork();
        nessieContainer = NessieContainer.builder().withNetwork(network).build();
        nessieContainer.start();
        tempDir = Files.createTempDirectory("test_trino_nessie_catalog");
        super.init();
    }

    @AfterClass
    public void teardown()
            throws IOException
    {
        network.close();
        nessieContainer.close();
        deleteRecursively(tempDir);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setIcebergProperties(
                        ImmutableMap.of(
                                "iceberg.catalog.type", "nessie",
                                "iceberg.nessie.uri", nessieContainer.getRestApiUri(),
                                "iceberg.nessie.warehouse", tempDir.toString()))
                .setSchemaInitializer(
                        SchemaInitializer.builder()
                                .withClonedTpchTables(REQUIRED_TPCH_TABLES)
                                .build())
                .build();
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .isEqualTo(String.format("" +
                                "CREATE TABLE iceberg.%1$s.region (\n" +
                                "   regionkey bigint,\n" +
                                "   name varchar,\n" +
                                "   comment varchar\n" +
                                ")\n" +
                                "WITH (\n" +
                                "   format = 'ORC',\n" +
                                "   location = '%2$s/%1$s.db/region'\n" +
                                ")",
                        schemaName,
                        tempDir.toString()));
    }

    @Test
    @Override
    public void testView()
    {
        assertThatThrownBy(super::testView)
                .hasStackTraceContaining("createView is not supported for Iceberg Nessie catalogs");
    }

    @Test
    @Override
    public void testMaterializedView()
    {
        assertThatThrownBy(super::testMaterializedView)
                .hasStackTraceContaining("createMaterializedView is not supported for Iceberg Nessie catalogs");
    }

    @Test
    @Override
    public void testRenameSchema()
    {
        assertThatThrownBy(super::testRenameSchema)
                .hasStackTraceContaining("renameNamespace is not supported for Iceberg Nessie catalogs");
    }
}
