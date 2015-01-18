package rfx.core.util.math;

import java.util.Random;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

public class TestDescriptiveStatistics {

	public static void main(String[] args) {
		// Get a DescriptiveStatistics instance
		DescriptiveStatistics stats = new DescriptiveStatistics();

		// Add the data from the array
		for( int i = 0; i < 1000; i++) {
		        stats.addValue(new Random().nextInt(1000));
		}

		// Compute some statistics
		double mean = stats.getMean();		
		double median = stats.getPercentile(50);
		System.out.println(mean);
		System.out.println(median);
	}
}
