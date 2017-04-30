package com.isa.mapreduce.io;

import java.util.Iterator;
import java.util.TreeMap;


public class OutputCollector {
    private final RecordWriter<String, Long> recordWriter;

    private final TreeMap<String, Long> frequency = new TreeMap<>();

    public OutputCollector(RecordWriter<String, Long> recordWriter) {
        this.recordWriter = recordWriter;
    }

    public void collect(String key, Long value) {
        frequency.compute(key, (k, v) -> v == null ? value : v + value);
    }

    public void write() {
        for (Iterator<String> iter = frequency.keySet().iterator(); iter.hasNext(); ) {
            String key = iter.next();
            recordWriter.write(key, frequency.get(key));
        }

        recordWriter.close();
    }

//    public List<Path> map(String filePath) throws IOException {
//		BufferedReader reader = Files.newBufferedReader(Paths.get(filePath));
//		TreeMap<String, Integer> frequency = new TreeMap<>();
//		List<Path> subFiles = new ArrayList<>();
//		String line = null;
//
//		int fileCounter = 0;
//		long runningSize = 0;
//		while ((line = reader.readLine()) != null) {
//			frequency.compute(line, (k, v) -> v == null ? 1 : v + 1);
//			runningSize += line.length();
//			if (runningSize > partitionSize) {
//				writeToFile(Paths.get(tempDir, filePrefix + fileCounter), frequency);
//				subFiles.add(Paths.get(tempDir, filePrefix + fileCounter));
//				fileCounter++;
//				runningSize = 0;
//				frequency.clear();
//			}
//		}
//
//		reader.close();
//		if (runningSize > 0) {
//			writeToFile(Paths.get(tempDir, filePrefix + fileCounter), frequency);
//			subFiles.add(Paths.get(tempDir, filePrefix + fileCounter));
//		}
//
//		return subFiles;
//	}
//
//	private void writeToFile(Path filePath, TreeMap<String, Integer> frequency) {
//		try (ObjectOutputStream writer = new ObjectOutputStream(Files.newOutputStream(filePath, StandardOpenOption.CREATE, StandardOpenOption.WRITE))) {
//			for (Iterator<String> iter = frequency.keySet().iterator(); iter.hasNext();) {
//				String key = iter.next();
//				writer.writeObject(new Mapping(key, frequency.get(key)));
//			}
//
//			writer.writeObject(null);
//			writer.flush();
//			writer.close();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//	}
}
