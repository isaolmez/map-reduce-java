package com.isa.mapreduce.io;

import java.util.Iterator;
import java.util.TreeMap;


public class OutputCollector {
    private final RecordWriter<String, Long> recordWriter;

    // Use Mapping class to compare with key&count with TreeSet
    private final TreeMap<String, Long> frequency = new TreeMap<>();

    public OutputCollector(RecordWriter<String, Long> recordWriter) {
        this.recordWriter = recordWriter;
    }

    public void collect(String key, Long value) {
        frequency.compute(key, (k, v) -> v == null ? value : v + value);
    }

    public void write() {
        for (Iterator<String> iter = frequency.keySet().iterator(); iter.hasNext(); ) {
            String key = iter.next();
            recordWriter.write(key, frequency.get(key));
        }

        recordWriter.close();
    }
}
