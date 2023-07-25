package com.datastax.oss.sga.impl.storage.k8s.codestorage;

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
    private String bucketName = "sga-code-storage";
    private String endpoint = "s3.amazonaws.com";
    @JsonAlias({"accesskey", "access-key"})
    private String accessKey;
    @JsonAlias({"secretkey", "secret-key"})
    private String secretKey;
}
