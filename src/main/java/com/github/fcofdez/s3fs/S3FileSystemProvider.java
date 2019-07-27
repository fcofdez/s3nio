package com.github.fcofdez.s3fs;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileTime;
import java.nio.file.spi.FileSystemProvider;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

import static com.github.fcofdez.s3fs.Util.checkArgument;
import static com.github.fcofdez.s3fs.Util.nullOrEmpty;

public class S3FileSystemProvider extends FileSystemProvider {

    private static final String S3_SCHEME = "s3";

    private final ConcurrentMap<URI, S3FileSystem> fileSystems = new ConcurrentHashMap<>();

    @Override
    public String getScheme() {
        return S3_SCHEME;
    }

    @Override
    public FileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException {
        S3FileSystem fs = fileSystems.get(uri);

        if (fs != null)
            return fs;

        checkArgument(uri.getScheme().equalsIgnoreCase(S3_SCHEME), "S3 nio FS expects '%s' scheme but got '%s'", S3_SCHEME, uri.getScheme());

        checkArgument(uri.getPort() == -1 && nullOrEmpty(uri.getFragment()) && nullOrEmpty(uri.getQuery()) && nullOrEmpty(uri.getUserInfo()));

        String bucket = uri.getHost();

        S3FileSystem s3FileSystem = S3FileSystem.withEnv(env, this);

        fileSystems.put(uri, s3FileSystem);
        return s3FileSystem;
    }

    @Override
    public FileSystem getFileSystem(URI uri) {
        return fileSystems.get(uri);
    }

    @Override
    public Path getPath(URI uri) {
        S3FileSystem fs = fileSystems.computeIfAbsent(uri, this::fromURI);
        return S3Path.createPath(fs, uri.getPath());
    }

    private S3FileSystem fromURI(URI uri) {
        return S3FileSystem.withDynamicCredentials(this);
    }

    @Override
    public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        return S3SeekableByteChannel.create(path, options, attrs);
    }

    @Override
    public DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> filter) throws IOException {
        return null;
    }

    @Override
    public void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {

    }

    @Override
    public void delete(Path path) throws IOException {

    }

    @Override
    public void copy(Path source, Path target, CopyOption... options) throws IOException {

    }

    @Override
    public void move(Path source, Path target, CopyOption... options) throws IOException {

    }

    @Override
    public boolean isSameFile(Path path, Path path2) throws IOException {
        return false;
    }

    @Override
    public boolean isHidden(Path path) throws IOException {
        return false;
    }

    @Override
    public FileStore getFileStore(Path path) throws IOException {
        return null;
    }

    @Override
    public void checkAccess(Path path, AccessMode... modes) throws IOException {

    }

    @Override
    public <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
        return null;
    }

    @Override
    public <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) throws IOException {
        return null;
    }

    @Override
    public AsynchronousFileChannel newAsynchronousFileChannel(Path path, Set<? extends OpenOption> options, ExecutorService executor, FileAttribute<?>... attrs) throws IOException {
        return super.newAsynchronousFileChannel(path, options, executor, attrs);
    }

    @Override
    public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
        return null;
    }

    @Override
    public void setAttribute(Path path, String attribute, Object value, LinkOption... options) throws IOException {

    }
}
