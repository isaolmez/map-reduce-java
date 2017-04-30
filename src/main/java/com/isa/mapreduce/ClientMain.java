package com.isa.mapreduce;

import com.isa.mapreduce.context.JobContext;
import com.isa.mapreduce.io.OutputCollector;
import com.isa.mapreduce.job.Job;
import com.isa.mapreduce.job.JobConfiguration;
import com.isa.mapreduce.job.impl.DefaultJob;
import com.isa.mapreduce.map.Mapper;
import com.isa.mapreduce.reduce.Reducer;
import com.isa.mapreduce.report.Reporter;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Scanner;

public class ClientMain {
    private static final String LINE_COUNT_COUNTER = "Line Count";

    public static void main(String[] args) throws IOException {
        String source = "input_256000.txt";
        String sourcePath = Paths.get("/home/isa/", source).toString();
        String target = "result.txt";
        String targetPath = Paths.get("/home/isa/", target).toString();
        JobConfiguration jobConfiguration = JobConfiguration.get()
                .inputFile(Paths.get(sourcePath).toString())
                .partitionSize(1_000_000)
                .outputFile(Paths.get(targetPath).toString())
                .outputDelimiter("\t")
                .tempPath("/tmp")
                .tempFilePrefix("Part-")
                .build();

        Job job = new DefaultJob(new MyMapper(), new MyReducer());
        Reporter reporter = job.run(jobConfiguration);
        System.out.println(reporter.getCounter(LINE_COUNT_COUNTER));
    }

    private static class MyMapper implements Mapper {

        @Override
        public void map(String input, OutputCollector collector, JobContext context) {
            Scanner scanner = new Scanner(input);
            while (scanner.hasNext()) {
                collector.collect(scanner.next().trim(), 1L);
            }

            scanner.close();

            Reporter reporter = context.getReporter();
            reporter.incrementCounter(LINE_COUNT_COUNTER, 1);
        }
    }

    private static class MyReducer implements Reducer {

        @Override
        public void reduce(String key, Iterable<Long> values, OutputCollector collector, JobContext context) {
            long sum = 0;
            for (long value : values) {
                sum += value;
            }

            collector.collect(key, sum);
        }
    }
}
