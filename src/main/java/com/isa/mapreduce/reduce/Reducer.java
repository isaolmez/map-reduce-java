package com.isa.mapreduce.reduce;

import com.isa.mapreduce.context.JobContext;
import com.isa.mapreduce.io.OutputCollector;
import com.isa.mapreduce.io.RecordWriter;

public interface Reducer{
    void reduce(String key, Iterable<Long> values, OutputCollector collector, JobContext context);
}
