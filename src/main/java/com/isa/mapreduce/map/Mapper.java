package com.isa.mapreduce.map;


import com.isa.mapreduce.context.JobContext;
import com.isa.mapreduce.io.OutputCollector;

public interface Mapper {
    void map(String input, OutputCollector collector, JobContext context);
}
