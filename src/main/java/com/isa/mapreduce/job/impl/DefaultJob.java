package com.isa.mapreduce.job.impl;


import com.google.common.collect.ImmutableList;
import com.isa.mapreduce.context.JobContext;
import com.isa.mapreduce.external.IndexMinPQ;
import com.isa.mapreduce.io.InputSplitter;
import com.isa.mapreduce.io.OutputCollector;
import com.isa.mapreduce.io.RecordReader;
import com.isa.mapreduce.io.RecordWriter;
import com.isa.mapreduce.io.impl.*;
import com.isa.mapreduce.job.Job;
import com.isa.mapreduce.job.JobConfiguration;
import com.isa.mapreduce.map.Mapper;
import com.isa.mapreduce.model.InputSplit;
import com.isa.mapreduce.model.Mapping;
import com.isa.mapreduce.reduce.Reducer;
import com.isa.mapreduce.report.Reporter;
import com.isa.mapreduce.report.impl.SimpleReporter;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class DefaultJob implements Job {
    private final Mapper mapper;

    private final Reducer reducer;

    private final JobContext context;

    public DefaultJob(Mapper mapper, Reducer reducer) {
        this.mapper = mapper;
        this.reducer = reducer;
        this.context = new JobContext(new SimpleReporter());
    }

    @Override
    public Reporter run(JobConfiguration configuration) {
        List<Path> intermediateFiles = mapPhase(configuration);
        shufflePhase(intermediateFiles, configuration);
        reducePhase(configuration);
        return context.getReporter();
    }

    private List<Path> mapPhase(JobConfiguration configuration) {
        InputSplitter inputSplitter = new ByteLimitSplitter(configuration.getPartitionSize());
        List<InputSplit> splits = inputSplitter.getSplits(configuration.getInputFile());
        List<Path> intermediateFiles = new ArrayList<>();
        for (int i = 0; i < splits.size(); i++) {
            InputSplit split = splits.get(i);
            RecordReader<String> recordReader = new FileLineReader(split);
            Path intermediateFile = Paths.get(String.format("%s/%s%s", configuration.getTempPath(), configuration.getTempFilePrefix(), i));
            RecordWriter<String, Long> recordWriter = new FileObjectWriter<>(intermediateFile);
            OutputCollector outputCollector = new OutputCollector(recordWriter);

            String record;
            while ((record = recordReader.read()) != null) {
                mapper.map(record, outputCollector, context);
            }

            outputCollector.write();
            intermediateFiles.add(intermediateFile);
        }

        return intermediateFiles;
    }

    private void shufflePhase(List<Path> intermediateFiles, JobConfiguration configuration) {
        IndexMinPQ<Mapping<String, Long>> indexMinPQ = new IndexMinPQ<>(intermediateFiles.size());
        FileObjectReader<Mapping<String, Long>>[] readers = new FileObjectReader[intermediateFiles.size()];
        Path combinedFile = Paths.get(String.format("%s/%s", configuration.getTempPath(), "combined"));
        FileObjectWriter<String, Long[]> writer = new FileObjectWriter<>(combinedFile);

        for (int i = 0; i < intermediateFiles.size(); i++) {
            Path intermediateFile = intermediateFiles.get(i);
            readers[i] = new FileObjectReader<>(new InputSplit(intermediateFile));
        }

        for (int i = 0; i < intermediateFiles.size(); i++) {
            indexMinPQ.insert(i, readers[i].read());
        }

        String runningKey = null;
        List<Long> runningValues = new ArrayList<>();
        Mapping<String, Long> currentMapping;

        while (!indexMinPQ.isEmpty()) {
            int min = indexMinPQ.minIndex();
            currentMapping = indexMinPQ.keyOf(min);
            if (runningKey != null) {
                if (runningKey.equals(currentMapping.getKey())) {
                    runningValues.add(currentMapping.getValue());
                } else {
                    writer.write(runningKey, runningValues.toArray(new Long[0]));
                    runningKey = currentMapping.getKey();
                    runningValues = new ArrayList<>();
                    runningValues.add(currentMapping.getValue());
                }
            } else {
                runningKey = currentMapping.getKey();
                runningValues.add(currentMapping.getValue());
            }

            indexMinPQ.delMin();
            Mapping<String, Long> newMapping = readers[min].read();
            if (newMapping != null) {
                indexMinPQ.insert(min, newMapping);
            }
        }

        writer.write(runningKey, runningValues.toArray(new Long[0]));
        writer.close();
    }

    private void reducePhase(JobConfiguration configuration) {
        Path combinedFile = Paths.get(String.format("%s/%s", configuration.getTempPath(), "combined"));
        FileObjectReader<Mapping<String, Long[]>> reader = new FileObjectReader<>(new InputSplit(combinedFile));
        Path outputFile = Paths.get(configuration.getOutputFile());
        FileLineWriter<String, Long> writer = new FileLineWriter<>(outputFile, configuration.getOutputDelimiter());
        OutputCollector collector = new OutputCollector(writer);

        Mapping<String, Long[]> record;
        while ((record = reader.read()) != null) {
            reducer.reduce(record.getKey(), ImmutableList.copyOf(record.getValue()), collector, context);
        }

        collector.write();
    }
}
