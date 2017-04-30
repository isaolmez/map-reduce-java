package com.isa.mapreduce.context;

import com.isa.mapreduce.report.Reporter;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class JobContext {
    private final Reporter reporter;

    public JobContext(Reporter reporter) {
        this.reporter = reporter;
    }

    public Reporter getReporter() {
        return reporter;
    }
}
