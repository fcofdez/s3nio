package com.github.fcofdez.s3fs;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.Map;
import java.util.Set;

public class S3FileSystem extends FileSystem {

    private static final String SEPARATOR = "" + UnixPath.SEPARATOR;

    private final FileSystemProvider provider;
    private final S3Client s3;
    private volatile boolean closed;

    private S3FileSystem(FileSystemProvider provider, S3Client s3) {
        this.provider = provider;
        this.s3 = s3;
    }

    static S3FileSystem withEnv(Map<String, ?> env, S3FileSystemProvider s3FileSystemProvider) {

        AwsBasicCredentials awsBasicCredentials = AwsBasicCredentials.create((String) env.get(S3FSSettings.ACCESS_KEY),
                                                                             (String) env.get(S3FSSettings.SECRET_KEY));
        StaticCredentialsProvider staticCredentialsProvider =
                StaticCredentialsProvider.create(awsBasicCredentials);

        S3Client s3 = S3Client.builder()
                              .region(Region.of((String) env.get(S3FSSettings.REGION)))
                              .credentialsProvider(staticCredentialsProvider)
                              .build();

        return new S3FileSystem(s3FileSystemProvider, s3);
    }

    static S3FileSystem withDynamicCredentials(S3FileSystemProvider s3FileSystemProvider) {
        DefaultCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();
        // extract region from uri

        S3Client s3 = S3Client.builder()
                              .region(Region.US_EAST_1)
                              .credentialsProvider(credentialsProvider)
                              .build();
        return new S3FileSystem(s3FileSystemProvider, s3);
    }

    @Override
    public FileSystemProvider provider() {
        return provider;
    }

    @Override
    public void close() throws IOException {
        if (closed)
            return;

        closed = true;
        s3.close();
    }

    @Override
    public boolean isOpen() {
        return !closed;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public String getSeparator() {
        return SEPARATOR;
    }

    @Override
    public Iterable<Path> getRootDirectories() {
        return null;
    }

    @Override
    public Iterable<FileStore> getFileStores() {
        return null;
    }

    @Override
    public Set<String> supportedFileAttributeViews() {
        return null;
    }

    @Override
    public Path getPath(String first, String... more) {
        return S3Path.createPath(this, first, more);
    }

    @Override
    public PathMatcher getPathMatcher(String syntaxAndPattern) {
        return null;
    }

    @Override
    public UserPrincipalLookupService getUserPrincipalLookupService() {
        return null;
    }

    @Override
    public WatchService newWatchService() throws IOException {
        return null;
    }

    public S3Client getS3Client() {
        return s3;
    }
}
