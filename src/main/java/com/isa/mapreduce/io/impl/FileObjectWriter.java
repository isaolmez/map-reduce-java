package com.isa.mapreduce.io.impl;

import com.isa.mapreduce.io.RecordWriter;
import com.isa.mapreduce.model.Mapping;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class FileObjectWriter<K extends Serializable & Comparable<K>, V extends Serializable> implements RecordWriter<K, V> {

    private final Path path;

    private final ObjectOutputStream writer;

    public FileObjectWriter(Path path) {
        this.path = path;
        try {
            this.writer = new ObjectOutputStream(Files.newOutputStream(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(K key, V value) {
        try {
            Mapping<K, V> mapping = new Mapping<>(key, value);
            writer.writeObject(mapping);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            writer.writeObject(null);
            writer.flush();
            writer.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
