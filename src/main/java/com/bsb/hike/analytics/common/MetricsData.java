package com.bsb.hike.analytics.common;

import org.json.simple.JSONObject;


public class MetricsData {
	public String metric;
	public Object value;
	public JSONObject tags;
	public Long timestamp;
	
	public MetricsData() {
	}
	
	public static  MetricsData set(String metric, Object value, JSONObject tags, String file){
		return set(metric, value, tags, System.currentTimeMillis()/1000L, file);
	}
	
	public static MetricsData set(String metric, Object value, JSONObject tags, Long timestamp, String file){
		MetricsData metricsData = new MetricsData();
		metricsData.metric = metric;
		metricsData.value = value;
		metricsData.timestamp = timestamp;
		if(tags == null){
			tags = new JSONObject();
		}
		metricsData.tags = tags;
		String ipAddress = "localhost";
		tags.put("ip", ipAddress);
		tags.put("file", file);
		return metricsData;
	}
	
	@Override
	public String toString(){
		StringBuilder builder = new StringBuilder();
		builder.append("MetricsData { " + "metric="+metric);
		builder.append(",value="+value);
		builder.append(",tags="+tags.toJSONString());
		builder.append(",timestamp="+timestamp);
		builder.append("}");
		return builder.toString();
	}

}
