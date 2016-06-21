package com.isa.mapreduce.sentence;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

import com.isa.mapreduce.pq.IndexMinPQ;

public class SentenceFrequencyCounter {
	private static String HomeDirectory;
	static {
		HomeDirectory = System.getProperty("user.home");
	}
	
	private int partitionSize = 10_000_000;
	private String filePrefix = "Part-";
	private String finalDelimiter = "\t";
	private String tempDir = "/tmp";

	public SentenceFrequencyCounter(int partitionSize, String tempDir) {
		this.partitionSize = partitionSize;
		this.tempDir = tempDir;
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
				fileCounter++;
				runningSize = 0;
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

	private void writeToFile(Path filePath, TreeMap<String, Integer> frequency) {
		try (ObjectOutputStream writer = new ObjectOutputStream(Files.newOutputStream(filePath, StandardOpenOption.CREATE, StandardOpenOption.WRITE))) {
			for (Iterator<String> iter = frequency.keySet().iterator(); iter.hasNext();) {
				String key = iter.next();
				writer.writeObject(new Mapping(key, frequency.get(key)));
			}

			writer.writeObject(null);
			writer.flush();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void reduce(List<Path> subFiles, String filePath) throws IOException, ClassNotFoundException {
		IndexMinPQ<Mapping> indexMinPQ = new IndexMinPQ<>(subFiles.size());
		ObjectInputStream[] readers = new ObjectInputStream[subFiles.size()];
		BufferedWriter writer = Files.newBufferedWriter(Paths.get(filePath));

		for (int i = 0; i < subFiles.size(); i++) {
			readers[i] = new ObjectInputStream(Files.newInputStream(subFiles.get(i)));
		}

		String line = null;
		String[] parts;
		for (int i = 0; i < subFiles.size(); i++) {
			indexMinPQ.insert(i, (Mapping) (readers[i].readObject()));
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
			Object o = readers[min].readObject();
			if (o != null) {
				indexMinPQ.insert(min, (Mapping) o);
			}
		}

		writer.write(String.format("%s%s%s", runningKey, finalDelimiter, runningValue));
		writer.newLine();
		writer.close();

		// Close the readers
		for (int i = 0; i < subFiles.size(); i++) {
			readers[i].close();
		}
		
		// Delete the temporary files
		for (int i = 0; i < subFiles.size(); i++) {
			Files.delete(subFiles.get(i));
		}

	}

	public void run(String sourcePath, String targetPath) throws IOException, ClassNotFoundException {
		reduce(map(sourcePath), targetPath);
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException {
		String source = "test.txt";
		String sourcePath = Paths.get(HomeDirectory, source).toString();
		String target = "result.txt";
		String targetPath = Paths.get(HomeDirectory, target).toString();
		SentenceFrequencyCounter frequencyCounter = new SentenceFrequencyCounter(1000_000, HomeDirectory);
		frequencyCounter.run(sourcePath, targetPath);
	}
}
