package com.isa.counting.mapreduce.main;

import java.io.IOException;
import java.nio.file.Paths;

import com.isa.counting.mapreduce.perf.DoublingTestTemplate;
import com.isa.counting.mapreduce.perf.InputFileGenerator;
import com.isa.counting.mapreduce.sentence.SentenceFrequencyCounter;

public class SentenceCountDoublingTest extends DoublingTestTemplate {

	private static String HomeDirectory;
	static {
		HomeDirectory = System.getProperty("user.home");
	}

	@Override
	public void methodToBePerformed(int N) {
		SentenceFrequencyCounter frequencyCounter = new SentenceFrequencyCounter(1000_000, HomeDirectory);
		String source = String.format("%s%s%s", "input_", N, ".txt");
		String sourcePath = Paths.get(HomeDirectory, source).toString();
		String target = String.format("%s%s%s", "result_", N, ".txt");
		String targetPath = Paths.get(HomeDirectory, target).toString();
		try {
			frequencyCounter.run(sourcePath, targetPath);
		} catch (ClassNotFoundException | IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws ClassNotFoundException, IOException {
		int trialCount = 18;

		for (int i = 0; i < trialCount + 1; i++) {
			InputFileGenerator.generate(i, HomeDirectory);
		}

		SentenceCountDoublingTest doublingTest = new SentenceCountDoublingTest();
		doublingTest.ratio(trialCount);
	}
}