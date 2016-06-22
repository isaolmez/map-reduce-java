package com.isa.counting.inmem.main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.TreeMap;

public class SentenceFrequencyCounterInMem {

	public void count(String inputPath, String targetPath) throws IOException {
		BufferedReader reader = Files.newBufferedReader(Paths.get(inputPath));
		TreeMap<String, Integer> frequency = new TreeMap<>();
		String line = null;

		while ((line = reader.readLine()) != null) {
			frequency.compute(line, (k, v) -> v == null ? 1 : v + 1);
		}

		reader.close();

		BufferedWriter writer = Files.newBufferedWriter(Paths.get(targetPath));
		for (Iterator<String> iter = frequency.keySet().iterator(); iter.hasNext();) {
			String key = iter.next();
			writer.write(String.format("%s\t%s", key, frequency.get(key)));
			writer.newLine();
		}

		writer.close();
	}

	public static void main(String[] args) throws IOException {
		 String source = "test.txt";
		 String sourcePath = Paths.get("/home/isa/", source).toString();
		 String target = "result.txt";
		 String targetPath = Paths.get("/home/isa/", target).toString();
		 SentenceFrequencyCounterInMem frequencyCounter = new SentenceFrequencyCounterInMem();
		 frequencyCounter.count(sourcePath, targetPath);
	}
}
