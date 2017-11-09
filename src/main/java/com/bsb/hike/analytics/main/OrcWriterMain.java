package com.bsb.hike.analytics.main;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bsb.hike.analytics.writer.OrcWriter;


public class OrcWriterMain {

	private static final Logger logger = LoggerFactory.getLogger(OrcWriterMain.class);
    
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		if(args.length < 2){
        		logger.info("Usage::OrcWriterMain srcFile destFile");
            System.exit(1);
        }
		
		try {
			logger.info("Initiate Orc Writer ");
			OrcWriter writer = new OrcWriter(args[0],args[1]);
			long start=System.currentTimeMillis();
			logger.info("Write to Orc:"+System.currentTimeMillis());
			writer.write();
			writer.close();
			long stop = System.currentTimeMillis();
			System.out.println(stop-start);
			logger.info("Close Orc:"+System.currentTimeMillis());
		}catch(Exception ex) {
			System.out.println("Exception in writer");
			logger.error("Error in converting to orc file "+ex.getMessage());
			System.exit(1);
		}
		System.exit(0);
	}

}
