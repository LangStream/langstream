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
package ai.langstream.impl.storage.k8s.codestorage;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AzureBlobCodeStorageConfiguration {
    private String type;

    private String container = "langstream-code-storage";

    private String endpoint;

    @JsonAlias({"sastoken"})
    @JsonProperty("sas-token")
    private String sasToken;

    @JsonAlias({"storageaccountname"})
    @JsonProperty("storage-account-name")
    private String storageAccountName;

    @JsonAlias({"storageaccountkey"})
    @JsonProperty("storage-account-key")
    private String storageAccountKey;

    @JsonAlias({"storageaccountconnectionstring"})
    @JsonProperty("storage-account-connection-string")
    private String storageAccountConnectionString;
}
