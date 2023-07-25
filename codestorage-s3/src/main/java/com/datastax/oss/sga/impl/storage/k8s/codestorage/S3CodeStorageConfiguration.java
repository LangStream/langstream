package com.datastax.oss.sga.impl.storage.k8s.codestorage;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class S3CodeStorageConfiguration {
    private String bucketName = "sga-code-storage";
    private String endpoint = "s3.amazonaws.com";
    private String accessKey;
    private String secretKey;
}
