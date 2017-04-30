package com.isa.mapreduce.io;

import com.isa.mapreduce.model.InputSplit;

import java.util.List;

public interface InputSplitter {
    List<InputSplit> getSplits(String inputFile);
}
