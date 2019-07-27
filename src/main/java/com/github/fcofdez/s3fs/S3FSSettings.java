package com.github.fcofdez.s3fs;

import java.util.HashMap;
import java.util.Map;

public class S3FSSettings {

    public static final String REGION = "region";
    public static final String ACCESS_KEY = "access_key";
    public static final String SECRET_KEY = "secret_key";

    private final String region;
    private final String accessKey;
    private final String secretKey;
    private final String bucket;

    private S3FSSettings(String region, String accessKey, String secretKey, String bucket) {
        this.region = region;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.bucket = bucket;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private String region;
        private String accessKey;
        private String secretKey;
        private String bucket;

        public Builder setRegion(String region) {
            this.region = region;
            return this;
        }

        public Builder setAccessKey(String accessKey) {
            this.accessKey = accessKey;
            return this;
        }

        public Builder setSecretKey(String secretKey) {
            this.secretKey = secretKey;
            return this;
        }

        public Builder setBucket(String bucket) {
            this.bucket = bucket;
            return this;
        }

        public S3FSSettings createS3FSSettings() {
            return new S3FSSettings(region, accessKey, secretKey, bucket);
        }
    }

    public Map<String, ?> env()
    {
        Map<String, Object> env = new HashMap<>();
        env.put(REGION, region);
        env.put(ACCESS_KEY, accessKey);
        env.put(SECRET_KEY, secretKey);
        return env;
    }
}
