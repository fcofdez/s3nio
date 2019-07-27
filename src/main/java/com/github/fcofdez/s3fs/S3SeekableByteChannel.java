package com.github.fcofdez.s3fs;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.Set;

public class S3SeekableByteChannel implements SeekableByteChannel {

    public static final int BUFFER_SIZE = 1 << 12;

    private byte[] buffer = new byte[BUFFER_SIZE];
    private int bufferPointer = 0;
    private int bufferLimit = 0;
    private long position = 0;
    private final long size;
    private final S3Client s3Client;
    private final String bucket;
    private final String key;
    private boolean closed = false;

    private S3SeekableByteChannel(S3Client s3Client, String bucket, String key) {
        this.bucket = bucket;
        this.key = key;
        this.s3Client = s3Client;
        HeadObjectRequest build = HeadObjectRequest.builder().bucket(bucket).key(key).build();
        HeadObjectResponse headObjectResponse = s3Client.headObject(build);

        this.size = headObjectResponse.contentLength();
    }

    static SeekableByteChannel create(Path path, Set<? extends OpenOption> options, FileAttribute<?>[] attrs) {
        if (path instanceof S3Path)
        {
            S3Path s3Path = (S3Path) path;
            S3FileSystem s3FileSystem = (S3FileSystem) s3Path.getFileSystem();
            return new S3SeekableByteChannel(s3FileSystem.getS3Client(), s3Path.getBucket(), s3Path.getKey());
        }
        return null;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        if (position == size)
            return -1;

        if (remaining() == 0) {
            rebuffer();
        }

        int initialPosition = dst.position();

        while (remaining() > 0 && dst.remaining() > 0) {
            int len = Math.min(remaining(), dst.remaining());
            dst.put(buffer, bufferPointer, len);
            bufferPointer += (dst.position() - initialPosition);
            position += (dst.position() - initialPosition);
        }

        return dst.position() - initialPosition;
    }

    private void rebuffer() throws IOException {
        GetObjectRequest rangeReq = GetObjectRequest.builder()
                                                    .bucket(bucket)
                                                    .key(key)
                                                    .range(buildRangeRequest())
                                                    .build();

        ResponseInputStream<GetObjectResponse> in = s3Client.getObject(rangeReq);
        int bytesRead = 0;
        int n;
        while ((n = in.read(buffer)) > -1) {
            bytesRead += n;
        }
        bufferPointer = 0;
        bufferLimit = bytesRead;
    }

    private String buildRangeRequest() {
        // upper range inclusive
        long upperBound = Math.min(size, position + BUFFER_SIZE - 1);
        return String.format("bytes=%d-%d", position, upperBound);
    }

    private int remaining() {
        return bufferLimit - bufferPointer;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        src.remaining();
        return 0;
    }

    @Override
    public long position() throws IOException {
        return position;
    }

    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
        // check position is within current range;
        this.position = newPosition;
        // invalidate buffer
        return this;
    }

    @Override
    public long size() throws IOException {
        return size;
    }

    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isOpen() {
        return !closed;
    }

    @Override
    public void close() throws IOException {
        closed = true;
    }
}
