package com.isa.mapreduce.job;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

public class JobConfiguration {
    private final String inputFile;
    private final long partitionSize;
    private final String outputFile;
    private final String outputDelimiter;
    private final String tempPath;
    private final String tempFilePrefix;

    public JobConfiguration(String inputFile, long partitionSize, String outputFile, String outputDelimiter, String tempPath, String tempFilePrefix) {
        this.inputFile = inputFile;
        this.partitionSize = partitionSize;
        this.outputFile = outputFile;
        this.outputDelimiter = outputDelimiter;
        this.tempPath = tempPath;
        this.tempFilePrefix = tempFilePrefix;
    }

    public String getInputFile() {
        return inputFile;
    }

    public long getPartitionSize() {
        return partitionSize;
    }

    public String getOutputFile() {
        return outputFile;
    }

    public String getOutputDelimiter() {
        return outputDelimiter;
    }

    public String getTempPath() {
        return tempPath;
    }

    public String getTempFilePrefix() {
        return tempFilePrefix;
    }

    public static Builder get() {
        return new Builder();
    }

    public static class Builder {
        private String inputFile;
        private long partitionSize = 10_000_000;
        private String outputFile;
        private String outputDelimiter = "\t";
        private String tempPath = "/tmp";
        private String tempFilePrefix = "Part-";

        public Builder inputFile(String inputFile) {
            Preconditions.checkArgument(StringUtils.isNotBlank(inputFile));
            this.inputFile = inputFile;
            return this;
        }

        public Builder partitionSize(long partitionSize) {
            Preconditions.checkArgument(partitionSize > 0);
            this.partitionSize = partitionSize;
            return this;
        }

        public Builder outputFile(String outputFile) {
            Preconditions.checkArgument(StringUtils.isNotBlank(outputFile));
            this.outputFile = outputFile;
            return this;
        }

        public Builder outputDelimiter(String outputDelimiter) {
            Preconditions.checkArgument(StringUtils.isNotEmpty(outputDelimiter));
            this.outputDelimiter = outputDelimiter;
            return this;
        }

        public Builder tempPath(String tempPath) {
            Preconditions.checkArgument(StringUtils.isNotBlank(tempPath));
            this.tempPath = tempPath;
            return this;
        }

        public Builder tempFilePrefix(String tempFilePrefix) {
            Preconditions.checkArgument(StringUtils.isNotBlank(tempFilePrefix));
            this.tempFilePrefix = tempFilePrefix;
            return this;
        }

        public JobConfiguration build() {
            return new JobConfiguration(inputFile, partitionSize, outputFile, outputDelimiter, tempPath, tempFilePrefix);
        }
    }
}


