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
package ai.langstream.agents.s3;

import ai.langstream.ai.agents.commons.JsonRecord;
import ai.langstream.ai.agents.commons.MutableRecord;
import ai.langstream.api.runner.code.AbstractAgentCode;
import ai.langstream.api.runner.code.AgentProcessor;
import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.RecordSink;
import ai.langstream.api.runtime.ComponentType;
import com.samskivert.mustache.Mustache;
import com.samskivert.mustache.Template;
import io.minio.GetObjectArgs;
import io.minio.GetObjectResponse;
import io.minio.MinioClient;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class S3Processor extends AbstractAgentCode implements AgentProcessor {
    private String bucketName;
    private MinioClient minioClient;
    private Template objectTemplate;

    @Override
    public void init(Map<String, Object> configuration) throws Exception {
        bucketName = configuration.getOrDefault("bucketName", "langstream-source").toString();
        String endpoint =
                configuration
                        .getOrDefault("endpoint", "http://minio-endpoint.-not-set:9090")
                        .toString();
        String username = configuration.getOrDefault("access-key", "minioadmin").toString();
        String password = configuration.getOrDefault("secret-key", "minioadmin").toString();
        String region = configuration.getOrDefault("region", "").toString();
        String objectName = configuration.getOrDefault("objectName", "").toString();

        // Object name is a mustache template because it is passed in the record
        objectTemplate = Mustache.compiler().compile(objectName);

        log.info(
                "Connecting to S3 Bucket at {} in region {} with user {}",
                endpoint,
                region,
                username);

        MinioClient.Builder builder =
                MinioClient.builder().endpoint(endpoint).credentials(username, password);
        if (!region.isBlank()) {
            builder.region(region);
        }
        minioClient = builder.build();
    }

    @Override
    public void process(List<Record> records, RecordSink recordSink) {
        for (Record record : records) {
            processRecord(record, recordSink);
        }
    }

    @Override
    public ComponentType componentType() {
        return ComponentType.PROCESSOR;
    }

    private void processRecord(Record record, RecordSink recordSink) {
        log.debug("Processing record {}", record.toString());
        MutableRecord context = MutableRecord.recordToMutableRecord(record, true);
        final JsonRecord jsonRecord = context.toJsonRecord();

        String fileName = objectTemplate.execute(jsonRecord);
        if (log.isDebugEnabled()) {
            log.debug("Processing JSON record {}", jsonRecord.toString());
        }

        log.info("Processing file {}", fileName);

        // Retrieve headers from the original record
        Collection<Header> originalHeaders = record.headers();

        log.debug("Original headers: {}", originalHeaders);

        try {
            GetObjectArgs getObjectArgs =
                    GetObjectArgs.builder().bucket(bucketName).object(fileName).build();

            GetObjectResponse getObjectResponse = minioClient.getObject(getObjectArgs);

            // Read the file content from the response
            byte[] fileContent = getObjectResponse.readAllBytes();
            log.debug("File content: {}", new String(fileContent));

            // Create a new record with the file content
            Record processedRecord = new S3SourceRecord(fileContent, fileName, originalHeaders);
            log.debug("Processed record key: {}", processedRecord.key());
            log.debug("Processed record value: {}", processedRecord.value());
            // Send the processed record to the record sink
            recordSink.emit(new SourceRecordAndResult(record, List.of(processedRecord), null));
        } catch (Throwable e) {
            log.error("Error processing record: {}", e.getMessage());
            recordSink.emit(new SourceRecordAndResult(record, null, e));
        }
    }

    @Override
    protected Map<String, Object> buildAdditionalInfo() {
        return Map.of("bucketName", bucketName);
    }

    private static class S3SourceRecord implements Record {
        private final byte[] read;
        private final String name;
        private final Collection<Header> headers;

        public S3SourceRecord(byte[] read, String name, Collection<Header> originalHeaders) {
            this.read = read;
            this.name = name;
            this.headers = new ArrayList<>(originalHeaders);
            log.debug("Headers: {}", headers);
            this.headers.add(new S3RecordHeader("name", name));
        }

        /**
         * the key is used for routing, so it is better to set it to something meaningful. In case
         * of retransmission the message will be sent to the same partition.
         *
         * @return the key
         */
        @Override
        public Object key() {
            return name;
        }

        @Override
        public Object value() {
            return read;
        }

        @Override
        public String origin() {
            return null;
        }

        @Override
        public Long timestamp() {
            return System.currentTimeMillis();
        }

        @Override
        public Collection<Header> headers() {
            return headers;
        }

        @AllArgsConstructor
        @ToString
        private static class S3RecordHeader implements Header {

            final String key;
            final String value;

            @Override
            public String key() {
                return key;
            }

            @Override
            public String value() {
                return value;
            }

            @Override
            public String valueAsString() {
                return value;
            }
        }
    }
}
