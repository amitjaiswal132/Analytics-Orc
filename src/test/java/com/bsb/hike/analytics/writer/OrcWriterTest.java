package com.bsb.hike.analytics.writer;

import static org.junit.Assert.assertTrue;

import java.io.File;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrcWriterTest {
	
	private static final Logger logger = LoggerFactory.getLogger(OrcWriterTest.class);
    
	@Test
	public void convertToOrcTest() {
		String srcPath = "OrcTestData1.text";
		ClassLoader classLoader = ClassLoader.getSystemClassLoader();
		File srcFile = new File(classLoader.getResource(srcPath).getFile());
		String destFile = srcFile.getAbsolutePath().replaceAll(".text", ".orc");
		File orcFile=new File(destFile);
		if(orcFile.exists()) {
			orcFile.delete();
		}
		
		try {
			logger.info("Initiate Orc Writer ");
			//OrcWriter writer = new OrcWriter(srcFile.getAbsolutePath(),destFile, "struct<name:string,age:int>","STRING,INT");
			OrcWriter writer = new OrcWriter(srcFile.getAbsolutePath(),destFile);
			logger.info("Write:"+System.currentTimeMillis());
			writer.write();
			logger.info("Close:"+System.currentTimeMillis());
			writer.close();
		}catch(Exception ex) {
			System.out.println("Exception in writer");
			logger.error("Error in converting to orc file "+ex.getMessage());
		}
		logger.info(new File(destFile).getAbsolutePath());
		assertTrue("Orc file created ", new File(destFile).exists());

	}
	
}

