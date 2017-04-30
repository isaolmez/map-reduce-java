package com.isa.mapreduce.model;

import java.io.Serializable;

public class Mapping<K extends Serializable & Comparable<K>, V extends Serializable> implements Comparable<Mapping<K, V>>, Serializable {
    private static final long serialVersionUID = 1L;

    private final K key;

    private final V value;

    public Mapping(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    @Override
    public int compareTo(Mapping<K, V> target) {
        return this.key.compareTo(target.key);
    }
}