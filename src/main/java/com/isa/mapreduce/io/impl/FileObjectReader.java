package com.isa.mapreduce.io.impl;

import com.isa.mapreduce.io.RecordReader;
import com.isa.mapreduce.model.InputSplit;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Reader;
import java.nio.file.Files;

public class FileObjectReader<V extends Comparable<V>> implements RecordReader<V> {
    private final InputSplit inputSplit;

    private final ObjectInputStream reader;

    public FileObjectReader(InputSplit inputSplit) {
        this.inputSplit = inputSplit;
        try {
            this.reader = new ObjectInputStream(Files.newInputStream(inputSplit.getPath()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public V read() {
        try {
            return (V) reader.readObject();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            this.reader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
