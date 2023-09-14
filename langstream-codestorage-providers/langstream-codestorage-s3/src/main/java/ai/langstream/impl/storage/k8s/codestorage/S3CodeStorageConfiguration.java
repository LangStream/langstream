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
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class S3CodeStorageConfiguration {
    private String type;

    @JsonAlias({"bucketname", "bucket-name"})
    private String bucketName = "langstream-code-storage";

    private String endpoint = "s3.amazonaws.com";

    @JsonAlias({"accesskey", "access-key"})
    private String accessKey;

    @JsonAlias({"secretkey", "secret-key"})
    private String secretKey;
}
