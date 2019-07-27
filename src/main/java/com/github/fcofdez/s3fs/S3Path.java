package com.github.fcofdez.s3fs;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.util.Iterator;

class S3Path implements Path {
    private final S3FileSystem fileSystem;

    private final UnixPath unixPath;

    private S3Path(S3FileSystem s3FileSystem, UnixPath unixPath) {
        this.fileSystem = s3FileSystem;
        this.unixPath = unixPath;
    }

    static S3Path createPath(S3FileSystem s3FileSystem, String path, String... rest)
    {
        return new S3Path(s3FileSystem, UnixPath.createPath(path));
    }

    @Override
    public FileSystem getFileSystem() {
        return fileSystem;
    }

    @Override
    public boolean isAbsolute() {
        return false;
    }

    @Override
    public Path getRoot() {
        return S3Path.createPath(fileSystem, "/");
    }

    @Override
    public Path getFileName() {
        return newPath(unixPath.getFileName());
    }

    @Override
    public Path getParent() {
        return newPath(unixPath.getParent());
    }

    @Override
    public int getNameCount() {
        return 0;
    }

    @Override
    public Path getName(int index) {
        return null;
    }

    @Override
    public Path subpath(int beginIndex, int endIndex) {
        return null;
    }

    @Override
    public boolean startsWith(Path other) {
        S3Path s3Path = getS3Path(other);
        return unixPath.startsWith(s3Path.unixPath);
    }

    @Override
    public boolean startsWith(String other) {
        return unixPath.startsWith(UnixPath.createPath(other));
    }

    @Override
    public boolean endsWith(Path other) {
        S3Path s3Path = getS3Path(other);
        return unixPath.endsWith(s3Path.unixPath);
    }

    @Override
    public boolean endsWith(String other) {
        return false;
    }

    @Override
    public Path normalize() {
        return newPath(unixPath.normalize());
    }

    @Override
    public Path resolve(Path other) {
        S3Path otherS3Path = getS3Path(other);

        return newPath(unixPath.resolve(otherS3Path.unixPath));
    }

    private S3Path getS3Path(Path other)
    {
        if (other instanceof S3Path)
            return (S3Path) other;

        throw new IllegalArgumentException();
    }

    @Override
    public Path resolve(String other) {
        return null;
    }

    @Override
    public Path resolveSibling(Path other) {
        return null;
    }

    @Override
    public Path resolveSibling(String other) {
        return null;
    }

    @Override
    public Path relativize(Path other) {
        return null;
    }

    @Override
    public URI toUri() {
        return null;
    }

    @Override
    public Path toAbsolutePath() {
        return null;
    }

    @Override
    public Path toRealPath(LinkOption... options) throws IOException {
        return this;
    }

    private Path newPath(UnixPath unixPath) {
        return new S3Path(fileSystem, unixPath);
    }

    @Override
    public File toFile() {
        throw new UnsupportedOperationException();
    }

    @Override
    public WatchKey register(WatchService watcher, WatchEvent.Kind<?>[] events, WatchEvent.Modifier... modifiers) throws IOException {
        return null;
    }

    @Override
    public WatchKey register(WatchService watcher, WatchEvent.Kind<?>... events) throws IOException {
        return null;
    }

    @Override
    public Iterator<Path> iterator() {
        return null;
    }

    @Override
    public int compareTo(Path other) {
        return 0;
    }

    String getBucket() {
        return "datastax-backup-test";
    }

    String getKey() {
        return "2PCommit.tla";
    }
}
