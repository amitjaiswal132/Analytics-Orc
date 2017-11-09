package com.bsb.hike.analytics.writer;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.crunch.types.orc.OrcUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;

import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import com.bsb.hike.analytics.common.CommonUtils;
import com.bsb.hike.analytics.main.OrcWriterMain;

public class OrcWriter {
	
	private static final Logger logger = LoggerFactory.getLogger(OrcWriter.class);
	private String typeStr="struct<row_id:string,phylum:string,class:string,order:string,family:string,genus:string,species:string,time_stamp:timestamp,rec_id:string,val_int:int,val_str:string,device_id:string,from_user:string,to_user:string,fr_country:string,to_country:string,fr_operator:string,to_operator:string,fr_circle:string,to_circle:string,user_state:int,variety:string,form:string,record_id:string,race:string,breed:string,division:string,section:string,tribe:string,series:string,census:bigint,population:bigint,capacities:bigint,states:bigint,cts:bigint,nwtype:int,app_ver:string,dev_type:string,dev_os:string,os_ver:string,source:string,msisdn:string,log_type:string,src_ip:string>";;
	private  TypeInfo typeInfo= TypeInfoUtils.getTypeInfoFromTypeString(typeStr);;
	private  ObjectInspector inspector= OrcStruct.createObjectInspector(typeInfo);
	private static final long STRIPE_SIZE=100000;
	private static final int BUFFER_SIZE=100000;
	private String [] schema = {"STRING", "STRING", "STRING", "STRING",
        	"STRING","STRING", "STRING", "TIME_STAMP", "STRING", "INT", "STRING", "STRING",
            "STRING", "STRING", "STRING", "STRING","STRING","STRING","STRING","STRING",
            "INT", "STRING", "STRING", "STRING", "STRING", "STRING", "STRING", "STRING",
            "STRING", "STRING", "LONG", "LONG", "LONG", "LONG", "LONG", "INT",
            "STRING", "STRING", "STRING", "STRING", "STRING", "STRING","STRING", "STRING"};
	
	private String srcPath;
	private String destPath;
	private String crcPath;
	private Writer orcWriter;
	private boolean isWriterClosed = true;
	
	public OrcWriter(String srcPath, String destPath) throws IOException {
		// TODO Auto-generated constructor stub
		this.srcPath = srcPath;
		this.destPath = destPath;
		this.crcPath = Paths.get(destPath, ".crc").toString();
		initialize();
	}

	public OrcWriter(String srcPath, String destPath, String typeStr, String schema) throws IOException {
		// TODO Auto-generated constructor stub
		this.srcPath = srcPath;
		this.destPath = destPath;
		this.crcPath = Paths.get(destPath, ".crc").toString();
		this.typeStr = typeStr;
		this.typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeStr);
		this.inspector = OrcStruct.createObjectInspector(typeInfo);
		this.schema = schema.replaceAll(" ", "").split(",");
		initialize();
	}

	private void initialize() throws IOException {
		File destFile = new File(destPath);
		File crcFile = new File(crcPath);
		
		//Clean up orc file from dest path
		if(destFile.exists()) {
			destFile.delete();
		}
		
		if(crcFile.exists()) {
			crcFile.delete();
		}
		
		// create path for dest if not exists
		if(!destFile.getParentFile().exists()) {
			destFile.getParentFile().mkdir();
		}
		
		//create Orc writer
		Path tempPath = new Path(this.destPath);
		Configuration conf = new Configuration();
		orcWriter = OrcFile.createWriter(tempPath,
				OrcFile.writerOptions(conf).inspector(inspector).stripeSize(STRIPE_SIZE).bufferSize(BUFFER_SIZE));
		//mark writer as open
		this.isWriterClosed =false;
	}

	
	public void write() throws IOException {
		try {
			boolean status =  convertToOrc();
	        	if(status) {
	        		new File(crcPath).delete();
	        	}
	        	
		}catch(Exception e) {
			logger.error("Failed to convert to Orc file. Cleanup Orc file in any case"+destPath+" "+e.getMessage() +", "+ ExceptionUtils.getStackTrace(e));
	    		try {
				File orcFile = new File(destPath);
	    			File crcFile = new File(crcPath);
				if(orcFile.exists()) {
					orcFile.delete();
				}
				if(crcFile.exists()) {
					crcFile.delete();
				}
				
			this.close();	
	    		}catch(Exception ex) {
	    			logger.error("Failed to convert to delete file "+ this.destPath+ "  " + ex.getMessage() +   " , "+ ExceptionUtils.getStackTrace(e));
	    		}
		}
		
	}

	public void close() throws IOException {
		// TODO Auto-generated method stub
		if(!this.isWriterClosed) {
			orcWriter.close();
		}
		this.isWriterClosed=true;
	}
	
	private boolean convertToOrc() throws Exception {
		boolean ret=false;
		try {
			File file = new File(this.srcPath);
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader( new FileInputStream(file), "UTF8"));
			String line;
			int i=0;
			while ((line = bufferedReader.readLine()) != null ) {
				i=i+1;
				String [] in = line.split("\\u0001");
				Object[] olist = null;
				try {
					olist=CommonUtils.getOrcWritableObjects(in, schema);
					if(olist == null) {
						logger.info("Object return is null. logline : "+in.length+" # "+this.srcPath+" #"+ line);
						continue;
					}
					
					OrcStruct orcLine = OrcUtils.createOrcStruct(typeInfo, olist);
					orcWriter.addRow(orcLine);
					if(i<10) {
						logger.info("Data : "+line);
						orcWriter.writeIntermediateFooter();
							
					}
					if(i==2) {
						break;
					}
					
				}catch(Exception e) {
					logger.error("logline : " + line);
					logger.error("Failed to convert to Object "+ExceptionUtils.getStackTrace(e));
				}
			}
			logger.info("closing the buffer reader");
			bufferedReader.close();
		} catch (Exception e) {
			logger.error(String.format("Exception in converting to Object array "+e.getStackTrace().toString()));
			throw e;
		}
		return ret;
	}
	
}
