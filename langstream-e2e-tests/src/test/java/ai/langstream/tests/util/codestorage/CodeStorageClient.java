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
package ai.langstream.tests.util.codestorage;

import java.io.IOException;

public interface CodeStorageClient {

    void start();

    void createBucket(String bucketName) throws IOException;

    void deleteBucket(String bucketName) throws IOException;

    void uploadFromFile(String path, String bucketName, String objectName) throws IOException;

    boolean objectExists(String bucketName, String objectName) throws IOException;
}
