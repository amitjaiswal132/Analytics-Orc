package com.bsb.hike.analytics.common;

import java.sql.Timestamp;
import java.util.Arrays;

import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Strings;

import com.bsb.hike.analytics.writer.OrcWriter;

public class CommonUtils {
	private static final Logger logger = LoggerFactory.getLogger(OrcWriter.class);	
	public static Object[] getOrcWritableObjects(String [] inputs, String [] schema) {
        
        
        Object[] output = new Object[inputs.length];
        
        if(inputs.length!=schema.length)
        	{
        		logger.info("input length : "+inputs.length + " , schema.length:"+schema.length);
        		logger.info("Input row \n"+Arrays.toString(inputs));
        		
        		return null;
        }
        
        for(int i=0; i< schema.length; i++) {
            switch (schema[i]) {
                case "STRING":
                    if(!Strings.isNullOrEmpty(inputs[i])) {
                        output[i] = new Text(String.valueOf(inputs[i]).replace('\u0002','\n').replace('\u0003', '\r'));
                    }
                    else {
                        output[i] = new Text();
                    }
                    break;
                case "INT":
                    if(!Strings.isNullOrEmpty(inputs[i])) {
                    		output[i] = new IntWritable(Integer.valueOf((String) inputs[i]));
                    }
                    else {
                        output[i] = new IntWritable();
                    }
                    break;
                case "LONG":
                    if(!Strings.isNullOrEmpty(inputs[i])) {
                        output[i] = new LongWritable(Long.valueOf((String) inputs[i]));
                    }
                    else {
                        output[i] = new LongWritable();
                    }
                    break;
                case "TIME_STAMP":
                		//LOGGER.error("timestamp "+inputs[7].toString());
                    if(!Strings.isNullOrEmpty(inputs[i])) {	
                        output[i] = new TimestampWritable(Timestamp.valueOf(inputs[7].toString()));
                    }
                    else {
                        output[i] = new TimestampWritable();
                    }
                    break;
//                case "DATE":
//                    if(inputs[i] != null && inputs[i].toString().length()!=0) {
//                        output[i] = new DateWritable((Date)inputs[i]);
//                    }
//                    else {
//                        output[i] = new DateWritable();
//                    }
//                    break;
                default:
                    break;
            }
        }
        return output;
    }
}
