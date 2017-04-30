package com.isa.mapreduce.io;

public interface RecordWriter<K extends Comparable<K>, V> {
    void write(K key, V value);

    void close();
}
