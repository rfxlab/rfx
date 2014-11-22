package rfx.core.stream.data.metric;

import java.util.Map;

public class MetricStatisticsData {
	String metric;
	Map<Integer, Integer> statsPerMinute;
	Map<Integer, Integer> statsPerSecond;
		
	public MetricStatisticsData(String metric,
			Map<Integer, Integer> statsPerMinute,
			Map<Integer, Integer> statsPerSecond) {
		super();
		this.metric = metric;
		this.statsPerMinute = statsPerMinute;
		this.statsPerSecond = statsPerSecond;
	}
	
	public Map<Integer, Integer> getStatsPerMinute() {
		return statsPerMinute;
	}
	public void setStatsPerMinute(Map<Integer, Integer> statsPerMinute) {
		this.statsPerMinute = statsPerMinute;
	}
	public Map<Integer, Integer> getStatsPerSecond() {
		return statsPerSecond;
	}
	public void setStatsPerSecond(Map<Integer, Integer> statsPerSecond) {
		this.statsPerSecond = statsPerSecond;
	}	
}
