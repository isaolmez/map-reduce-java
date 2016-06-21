package com.isa.counting.mapreduce.sentence;

import java.io.Serializable;

public class Mapping implements Comparable<Mapping>, Serializable {
	private static final long serialVersionUID = 1L;
	protected String key;
	protected int value;

	public Mapping(String key, int value) {
		this.key = key;
		this.value = value;
	}

	@Override
	public int compareTo(Mapping target) {
		return this.key.compareTo(target.key);
	}
}