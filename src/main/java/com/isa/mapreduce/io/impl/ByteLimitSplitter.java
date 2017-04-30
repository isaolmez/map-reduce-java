package com.isa.mapreduce.io.impl;

import com.isa.mapreduce.io.InputSplitter;
import com.isa.mapreduce.model.InputSplit;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class ByteLimitSplitter implements InputSplitter {
    private final long byteSize;

    public ByteLimitSplitter(long byteSize) {
        this.byteSize = byteSize;
    }

    @Override
    public List<InputSplit> getSplits(String inputFile) {
        List<InputSplit> splits = new ArrayList<>();
        Path path = Paths.get(inputFile);
        long runningPointer = 0;
        long splitStartOffset = 0;
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(inputFile, "r")) {
            while (runningPointer < randomAccessFile.length()) {
                runningPointer += byteSize;
                if (runningPointer >= randomAccessFile.length()) {
                    splits.add(new InputSplit(path, splitStartOffset, randomAccessFile.length()));
                } else {
                    randomAccessFile.seek(runningPointer);
                    while (randomAccessFile.read() != '\n') {
                        runningPointer++;
                    }
                    splits.add(new InputSplit(path, splitStartOffset, runningPointer));
                    splitStartOffset = ++runningPointer;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return splits;
    }
}
