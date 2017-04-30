package com.isa.mapreduce.io.impl;

import com.isa.mapreduce.io.RecordWriter;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;

public class FileLineWriter<K extends Comparable<K>, V> implements RecordWriter<K, V> {

    private final Path path;

    private final String delimiter;

    private final PrintWriter writer;

    public FileLineWriter(Path path, String delimiter) {
        this.path = path;
        this.delimiter = delimiter;
        try {
            this.writer = new PrintWriter(Files.newOutputStream(path));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(K key, V value) {
        writer.println(String.format("%s%s%s", key, delimiter, value));
    }

    @Override
    public void close() {
        writer.close();
    }
}
