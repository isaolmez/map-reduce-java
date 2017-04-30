package com.isa.mapreduce.report.impl;

import com.isa.mapreduce.report.Reporter;

import java.util.LinkedHashMap;
import java.util.Map;

public class SimpleReporter implements Reporter {
    private Map<String, String> infoMap = new LinkedHashMap<>();

    private Map<String, String> warningMap = new LinkedHashMap<>();

    private Map<String, String> errorMap = new LinkedHashMap<>();

    private Map<String, Long> counterMap = new LinkedHashMap<>();

    @Override
    public void addInfo(String key, String value) {
        infoMap.put(key, value);
    }

    @Override
    public void addWarning(String key, String value) {
        warningMap.put(key, value);
    }

    @Override
    public void addError(String key, String value) {
        errorMap.put(key, value);
    }

    @Override
    public void incrementCounter(String counterName, long amount) {
        counterMap.compute(counterName, (k, v) -> v == null ? amount : v + amount);
    }

    @Override
    public long getCounter(String counterName) {
        return counterMap.getOrDefault(counterName, 0L);
    }
}
