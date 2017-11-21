package com.bsb.hike.analytics.common;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.codehaus.jackson.map.ObjectMapper;


public class PushMetrics {
	private static final Logger logger =  LoggerFactory.getLogger(PushMetrics.class);

	private PushMetrics() {
	}
	
	public static int send(MetricsData metricsData) throws ClientProtocolException, IOException{
		String url = "http://sniper.analytics.hike.in:4242/api/put";

		HttpClient client = HttpClientBuilder.create().setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD).build()).build();
		HttpPost post = new HttpPost(url);
		ObjectMapper mapper = new ObjectMapper();
		StringEntity entity = new StringEntity(mapper.writeValueAsString(metricsData));
		post.setEntity(entity);

		HttpResponse response = client.execute(post);
		logger.debug("MetricData : {}",metricsData);
		logger.debug("Response Code : "+ response.getStatusLine().getStatusCode());

		return -1;
	}
}

 