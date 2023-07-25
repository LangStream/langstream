package com.datastax.oss.sga.api.codestorage;


/**
 * This is an API to interact with the code storage.
 * The CodeStorage component is responsible for storing custom code for an application.
 */
public interface CodeStorage extends AutoCloseable {

    /**
     * Store the code for a given version of the application.
     * It is expected that the code is stored in a compressed archive.
     * The return value contains an Id that is to be used later to retrieve the code or to delete the package.
     * @param tenant The tenant
     * @param applicationId The application id
     * @param version The version of the application
     * @param codeArchive The code archive
     * @return The code store id
     */
    CodeArchiveMetadata storeApplicationCode(String tenant, String applicationId, String version, UploadableCodeArchive codeArchive) throws CodeStorageException;


    interface DownloadedCodeHandled {
        void accept(DownloadedCodeArchive archive) throws CodeStorageException;
    }

    /**
     * Download the code for a given version of the application.
     */
    void downloadApplicationCode(String tenant, String codeStoreId, DownloadedCodeHandled codeArchive) throws CodeStorageException;

    /**
     * Describe the code for a given version of the application.
     * @param tenant The tenant
     * @param codeStoreId The code store id
     * @return The code archive metadata
     */
    CodeArchiveMetadata describeApplicationCode(String tenant, String codeStoreId) throws CodeStorageException;

    /**
     * Delete the code for a given version of the application.
     * @param tenant The tenant
     * @param codeStoreId The code store id
     */
    void deleteApplicationCode(String tenant, String codeStoreId) throws CodeStorageException;

    /**
     * Delete all the versions of the application.
     * @param tenant The tenant
     * @param application The application
     */
    void deleteApplication(String tenant, String application) throws CodeStorageException;

    @Override
    void close();
}
