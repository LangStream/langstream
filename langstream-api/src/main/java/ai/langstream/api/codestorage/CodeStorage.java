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
package ai.langstream.api.codestorage;

/**
 * This is an API to interact with the code storage. The CodeStorage component is responsible for
 * storing custom code for an application.
 */
public interface CodeStorage extends AutoCloseable {

    /**
     * Store the code for a given version of the application. It is expected that the code is stored
     * in a compressed archive. The return value contains an Id that is to be used later to retrieve
     * the code or to delete the package.
     *
     * @param tenant The tenant
     * @param applicationId The application id
     * @param version The version of the application
     * @param codeArchive The code archive
     * @return The code store id
     */
    CodeArchiveMetadata storeApplicationCode(
            String tenant, String applicationId, String version, UploadableCodeArchive codeArchive)
            throws CodeStorageException;

    interface DownloadedCodeHandled {
        void accept(DownloadedCodeArchive archive) throws CodeStorageException;
    }

    /** Download the code for a given version of the application. */
    void downloadApplicationCode(
            String tenant, String codeStoreId, DownloadedCodeHandled codeArchive)
            throws CodeStorageException;

    /**
     * Describe the code for a given version of the application.
     *
     * @param tenant The tenant
     * @param codeStoreId The code store id
     * @return The code archive metadata
     */
    CodeArchiveMetadata describeApplicationCode(String tenant, String codeStoreId)
            throws CodeStorageException;

    /**
     * Delete the code for a given version of the application.
     *
     * @param tenant The tenant
     * @param codeStoreId The code store id
     */
    void deleteApplicationCode(String tenant, String codeStoreId) throws CodeStorageException;

    /**
     * Delete all the versions of the application.
     *
     * @param tenant The tenant
     * @param application The application
     */
    void deleteApplication(String tenant, String application) throws CodeStorageException;

    @Override
    void close();
}
