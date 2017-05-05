package com.isa.mapreduce.model;

import java.io.Serializable;
import java.util.Objects;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null) {
            return false;
        }

        if (this.getClass() != o.getClass()) {
            return false;
        }

        Mapping<?, ?> target = (Mapping<?, ?>) o;
        return Objects.equals(this.key, target.key)
                && Objects.equals(this.value, target.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }


}