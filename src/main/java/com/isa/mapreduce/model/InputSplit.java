package com.isa.mapreduce.model;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class InputSplit {
    private final Path path;

    private final long offset;

    private final long finishOffset;

    public InputSplit(Path path) {
        this(path, 0, -1);
    }

    public InputSplit(Path path, long offset, long finishOffset) {
        this.path = path;
        this.offset = offset;
        try {
            this.finishOffset = finishOffset != -1 ? finishOffset : Files.size(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Path getPath() {
        return path;
    }

    public long getOffset() {
        return offset;
    }

    public long getFinishOffset() {
        return finishOffset;
    }
}
