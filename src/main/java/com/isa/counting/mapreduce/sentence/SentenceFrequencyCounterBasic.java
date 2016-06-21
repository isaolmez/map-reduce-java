package com.isa.counting.mapreduce.sentence;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

import com.isa.counting.mapreduce.pq.IndexMinPQ;

public class SentenceFrequencyCounterBasic {
	private static String HomeDirectory;
	static {
		HomeDirectory = System.getenv(System.getProperty("user.home"));
	}
	
	

	private int partitionSize = 10_000_000;
	private String filePrefix = "Part-";
	private String intermediateDelimiter = "#";
	private String finalDelimiter = "\t";
	private String tempDir = "/home/isa/";

	public SentenceFrequencyCounterBasic(int partitionSize) {
		this.partitionSize = partitionSize;
	}

	public List<Path> map(String filePath) throws IOException {
		BufferedReader reader = Files.newBufferedReader(Paths.get(filePath));
		TreeMap<String, Integer> frequency = new TreeMap<>();
		List<Path> subFiles = new ArrayList<>();
		String line = null;

		int fileCounter = 0;
		long runningSize = 0;
		while ((line = reader.readLine()) != null) {
			frequency.compute(line, (k, v) -> v == null ? 1 : v + 1);
			runningSize += line.length();
			if (runningSize > partitionSize) {
				writeToFile(Paths.get(tempDir, filePrefix + fileCounter), frequency);
				subFiles.add(Paths.get(tempDir, filePrefix + fileCounter));
				runningSize = 0;
				fileCounter++;
				frequency.clear();
			}
		}

		reader.close();
		if (runningSize > 0) {
			writeToFile(Paths.get(tempDir, filePrefix + fileCounter), frequency);
			subFiles.add(Paths.get(tempDir, filePrefix + fileCounter));
		}

		return subFiles;
	}

	private void writeToFile(Path filePath, TreeMap<String, Integer> content) {
		try (BufferedWriter writer = Files.newBufferedWriter(filePath)) {
			for (Iterator<String> iter = content.keySet().iterator(); iter.hasNext();) {
				String key = iter.next();
				writer.write(String.format("%s%s%d", key, intermediateDelimiter, content.get(key)));
				writer.newLine();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void reduce(List<Path> subFiles, String filePath) throws IOException {
		IndexMinPQ<Mapping> indexMinPQ = new IndexMinPQ<>(subFiles.size());
		BufferedReader[] readers = new BufferedReader[subFiles.size()];
		BufferedWriter writer = Files.newBufferedWriter(Paths.get(filePath));

		for (int i = 0; i < subFiles.size(); i++) {
			readers[i] = Files.newBufferedReader(subFiles.get(i));
		}

		String line = null;
		String[] parts;
		for (int i = 0; i < subFiles.size(); i++) {
			line = readers[i].readLine();
			parts = line.split(intermediateDelimiter);
			indexMinPQ.insert(i, new Mapping(parts[0], Integer.parseInt(parts[1])));
		}

		String runningKey = null;
		int runningValue = 0;
		Mapping currentMapping = null;

		while (!indexMinPQ.isEmpty()) {
			int min = indexMinPQ.minIndex();
			currentMapping = indexMinPQ.keyOf(min);
			if (runningKey != null) {
				if (runningKey.equals(currentMapping.key)) {
					runningValue += currentMapping.value;
				} else {
					writer.write(String.format("%s%s%s", runningKey, finalDelimiter, runningValue));
					writer.newLine();
					runningKey = currentMapping.key;
					runningValue = currentMapping.value;
				}
			} else {
				runningKey = currentMapping.key;
				runningValue = currentMapping.value;
			}

			/** Delete the minimum key and add another entry from that file*/
			indexMinPQ.delMin();
			if ((line = readers[min].readLine()) != null) {
				parts = line.split(intermediateDelimiter);
				indexMinPQ.insert(min, new Mapping(parts[0], Integer.parseInt(parts[1])));
			}
		}

		writer.write(String.format("%s%s%s", runningKey, finalDelimiter, runningValue));
		writer.close();
		
		for (int i = 0; i < subFiles.size(); i++) {
			readers[i].close();
		}

	}

	public void run(String sourcePath, String targetPath) throws IOException {
		reduce(map(sourcePath), targetPath);
	}

	public static void main(String[] args) throws IOException {
		String source = "test.txt";
		String sourcePath = Paths.get("/home/isa/", source).toString();
		String target = "result.txt";
		String targetPath = Paths.get("/home/isa/", target).toString();
		SentenceFrequencyCounterBasic frequencyCounter = new SentenceFrequencyCounterBasic(1000_000);
		frequencyCounter.run(sourcePath, targetPath);
	}
}
