package com.isa.mapreduce.performance;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Random;

public class InputFileGenerator {
	private static String[] bucket1 = { "a", "b", "c", "d", "e", "f" };
	private static String[] bucket2 = { "g", "h", "l", "m", "n", "o" };
	private static String[] bucket3 = { "p", "q", "r", "s", "t", "u" };

	public static void generate(int level, String path) {
		int size = 1000 * (int) Math.pow(2, level);
		int cap = bucket1.length;
		Path p = Paths.get(path, String.format("%s%s%s", "input_", size, ".txt"));
		if (!Files.exists(p)) {
			try (BufferedWriter writer = Files.newBufferedWriter(p, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
				Random random = new Random();
				for (int i = 0; i < size; i++) {
					writer.write(String.format("%s\t%s\t%s", bucket1[random.nextInt(cap)] + level, bucket2[random.nextInt(cap)] + level,
							bucket3[random.nextInt(cap)] + level));
					writer.newLine();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
