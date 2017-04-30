package com.isa.mapreduce.job;

import com.isa.mapreduce.report.Reporter;

public interface Job {
    Reporter run(JobConfiguration jobConfiguration);
}
