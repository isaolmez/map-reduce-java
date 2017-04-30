package com.isa.mapreduce.io;

public interface RecordReader<V extends Comparable<V>> {
    V read();

    void close();
}
