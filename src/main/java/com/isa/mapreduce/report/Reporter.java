package com.isa.mapreduce.report;

public interface Reporter {
    void addInfo(String key, String value);

    void addWarning(String key, String value);

    void addError(String key, String value);

    void incrementCounter(String counterName, long amount);

    long getCounter(String counterName);
}
