package com.isa.mapreduce.io.impl;

import com.isa.mapreduce.io.RecordReader;
import com.isa.mapreduce.model.InputSplit;

import java.io.IOException;
import java.io.RandomAccessFile;

public class FileLineReader<V extends Comparable<V>> implements RecordReader<String> {
    private final InputSplit inputSplit;

    private final RandomAccessFile reader;

    public FileLineReader(InputSplit inputSplit) {
        this.inputSplit = inputSplit;
        try {
            this.reader = new RandomAccessFile(inputSplit.getPath().toFile(), "r");
            reader.seek(inputSplit.getOffset());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String read() {
        try {
            if(reader.getFilePointer() < inputSplit.getFinishOffset()){
                return reader.readLine();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return null;
    }

    @Override
    public void close() {
        try {
            reader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
