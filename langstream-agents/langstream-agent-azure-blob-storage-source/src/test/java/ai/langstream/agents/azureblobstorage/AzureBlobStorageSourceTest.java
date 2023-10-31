/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.langstream.agents.azureblobstorage;

import static org.junit.jupiter.api.Assertions.*;

import ai.langstream.api.runner.code.Record;
import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobContainerClient;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class AzureBlobStorageSourceTest {

    @Test
    @Disabled
    void test() throws Exception {
        final String endpoint = "";
        final String connectionString = "";

        AzureBlobStorageSource source = new AzureBlobStorageSource();
        final Map<String, Object> config =
                Map.of("endpoint", endpoint, "storage-account-connection-string", connectionString);
        final BlobContainerClient containerClient =
                AzureBlobStorageSource.createContainerClient(config);
        containerClient.getBlobClient("test.txt").deleteIfExists();
        containerClient.getBlobClient("test.txt").upload(BinaryData.fromString("test"));
        source.init(config);
        final List<Record> read = source.read();
        assertEquals(1, read.size());
        assertEquals("test", new String((byte[]) read.get(0).value()));
    }
}
