package com.isa.counting.mapreduce.perf;

/**
 * 
 * Inspired from Algorithms, Sedgewick
 *
 */

public abstract class DoublingTestTemplate {
	public double timeTrial(int N) { 

		Stopwatch timer = new Stopwatch();
		methodToBePerformed(N);
		return timer.elapsedTime();
	}

	public void run(int n){ // Print table of running times.
		for (int N = 1000; n-- > 0; N += N) { // Print time for problem size N.
			double time = timeTrial(N);
			System.out.printf("%7d %5.1f\n", N, time);
		}
	}

	public void run() { // Print table of running times.
		for (int N = 1000; true; N += N) { // Print time for problem size N.
			double time = timeTrial(N);
			System.out.printf("%7d %5.1f\n", N, time);
		}
	}

	public void ratio(int n) {
		double prev = timeTrial(1000);
		for (int N = 2000; n-- > 0; N += N) {
			double time = timeTrial(N);
			System.out.printf("%6d %7.1f ", N, time);
			System.out.printf("%5.3f ", time / prev);
			System.out.printf("%5.3f\n", Math.log(time / prev) / Math.log(2));
			prev = time;
		}
	}
	
	public void ratio() {
		double prev = timeTrial(1000);
		for (int N = 2000; true; N += N) {
			double time = timeTrial(N);
			System.out.printf("%6d %7.1f ", N, time);
			System.out.printf("%5.3f ", time / prev);
			System.out.printf("%5.3f\n", Math.log(time / prev) / Math.log(2));
			prev = time;
		}
	}
	
	public abstract void methodToBePerformed(int N);

}