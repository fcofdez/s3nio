package com.github.fcofdez.s3fs;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.github.fcofdez.s3fs.Util.checkArgument;

final class UnixPath implements CharSequence {
    static final char SEPARATOR = '/';
    private static final String ROOT = "" + SEPARATOR;
    private static final String CURRENT_DIR = ".";
    private static final String PARENT_DIR = "..";


    private static final UnixPath ROOT_PATH = new UnixPath(ROOT);
    private static final UnixPath EMPTY_PATH = new UnixPath("");

    private final String path;
    private List<String> splittedPath;

    private UnixPath(String path) {
        this.path = path;
    }

    public static UnixPath createPath(String path) {
        if (path.isEmpty())
            return EMPTY_PATH;

        if (isRootInternal(path))
            return ROOT_PATH;

        return new UnixPath(path);
    }

    private static boolean isRootInternal(String path) {
        return path.length() == 1 && path.charAt(0) == SEPARATOR;
    }

    public boolean isRoot() {
        return isRootInternal(path);
    }

    public boolean isAbsolute() {
        return !path.isEmpty() && path.charAt(0) == SEPARATOR;
    }

    /**
     * Returns {@code other} appended to {@code path}.
     *
     * @see java.nio.file.Path#resolve(java.nio.file.Path)
     */
    public UnixPath resolve(UnixPath other) {
        if (other.isAbsolute())
            return other;

        if (other.isEmpty())
            return this;

        if (hasTrailingSeparator())
            return new UnixPath(path + other);

        return new UnixPath(path + SEPARATOR + other);
    }

    /**
     * Returns parent directory (including trailing separator) or {@code null} if no parent remains.
     *
     * @see java.nio.file.Path#getParent()
     */
    public UnixPath getParent() {
        if (path.isEmpty() || isRoot())
            return null;

        int index = hasTrailingSeparator() ? path.lastIndexOf(SEPARATOR, path.length() - 2) : path.lastIndexOf(SEPARATOR);

        if (index == -1)
            return isAbsolute() ? ROOT_PATH : null;

        return new UnixPath(path.substring(0, index + 1));
    }

    private boolean hasTrailingSeparator() {
        return hasTrailingSeparatorInternal(path);
    }

    private static boolean hasTrailingSeparatorInternal(String path) {
        return !path.isEmpty() && path.charAt(path.length() - 1) == SEPARATOR;
    }

    /**
     * Returns {@code other} resolved against parent of {@code path}.
     *
     * @see java.nio.file.Path#resolveSibling(java.nio.file.Path)
     */
    public UnixPath resolveSibling(UnixPath other) {
        UnixPath parent = getParent();
        return parent == null ? other : parent.resolve(other);
    }

    public boolean isEmpty() {
        return path.isEmpty();
    }

    /**
     * Returns {@code other} made relative to {@code path}.
     *
     * @see java.nio.file.Path#relativize(java.nio.file.Path)
     */
    public UnixPath relativize(UnixPath other) {
        return other;
    }

    /**
     * Returns {@code true} if {@code path} starts with {@code other}.
     *
     * @see java.nio.file.Path#startsWith(java.nio.file.Path)
     */
    public boolean startsWith(UnixPath other) {
        return false;
    }

    /**
     * Returns {@code true} if {@code path} ends with {@code other}.
     *
     * @see java.nio.file.Path#endsWith(java.nio.file.Path)
     */
    public boolean endsWith(UnixPath other) {
        return false;
    }

    /**
     * Converts relative path to an absolute path.
     */
    public UnixPath toAbsolutePath(UnixPath currentWorkingDirectory) {
        checkArgument(currentWorkingDirectory.isAbsolute());
        return isAbsolute() ? this : currentWorkingDirectory.resolve(this);
    }

    /**
     * Returns {@code toAbsolutePath(ROOT_PATH)}.
     */
    public UnixPath toAbsolutePath() {
        return toAbsolutePath(ROOT_PATH);
    }

    @Override
    public int length() {
        return path.length();
    }

    @Override
    public char charAt(int index) {
        return path.charAt(index);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        return path.subSequence(start, end);
    }

    @Override
    public String toString() {
        return path;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UnixPath unixPath = (UnixPath) o;
        return Objects.equals(path, unixPath.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path);
    }

    UnixPath getFileName() {
        return this;
    }

    /**
     * /adasd/./asd/aasd/../
     * @return
     * @see Path#normalize()
     */
    UnixPath normalize() {
        int mark = 0;

        boolean mutated = false;
        int index;
        int resultSize = 0;
        List<String> parts = new ArrayList<>();
        do {
            index = path.indexOf(SEPARATOR, mark);

            String part = path.substring(mark, index == -1 ? path.length() : index + 1);

            switch (part) {
                case CURRENT_DIR:
                case CURRENT_DIR + SEPARATOR:
                    mutated = true;
                    break;
                case PARENT_DIR:
                case PARENT_DIR + SEPARATOR:
                    mutated = true;
                    if (!parts.isEmpty()) {
                        resultSize -= parts.remove(parts.size() - 1).length();
                    }
                    break;
                default:
                    if (index != mark || index == 0) {
                        parts.add(part);
                        resultSize += part.length();
                    } else {
                        mutated = true;
                    }
            }

            mark = index + 1;
        } while (index != -1);

        if (!mutated)
            return this;

        StringBuilder result = new StringBuilder(resultSize);
        for (String part : parts) {
            result.append(part);
        }

        return new UnixPath(result.toString());
    }
}
