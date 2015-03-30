package com.elex.ssp.mlctr;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.elex.ssp.mlctr.idx.FeatureValueEncoder;
import com.elex.ssp.mlctr.idx.IndexLoader;
import com.elex.ssp.mlctr.idx.PrepareForIndex;
import com.elex.ssp.mlctr.vector.FeatureVectorizer;
import com.elex.ssp.mlctr.vector.UserWordFeature;


public class Scheduler {
	
	private static final Logger log = LoggerFactory.getLogger(Scheduler.class);

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		AtomicInteger currentPhase = new AtomicInteger();
		String[] stageArgs = { otherArgs[0], otherArgs[1] };// 运行阶段控制参数
		int success = 0;
		
		// stage 0
		if (shouldRunNextPhase(stageArgs, currentPhase)) {
			log.info("prepare feature value for index encoder!!!");
			try{
				PrepareForIndex.prepareAll();
			}catch(Exception se){
				log.error("prepare feature value for index encoder ERROR!!!,SYSTEM EXIT!!!");
				System.exit(success);
			}			
			log.info("prepare feature value for index encoder SUCCESS!!!");
		}
		
		//stage 1
		if (shouldRunNextPhase(stageArgs, currentPhase)) {
			log.info("feature value encode START!!!");
			try{
				FeatureValueEncoder.processAll();
			}catch(Exception se){
				log.error("feature value encode ERROR!!!,SYSTEM EXIT!!!");
				System.exit(success);
			}		
			log.info("feature value encode SUCCESS!!!");
		}
		
		//stage 2
		if (shouldRunNextPhase(stageArgs, currentPhase)) {
			log.info("load index to hive and hbase START!!!");
			try{
				IndexLoader.load();
			}catch(Exception se){
				log.error("load index to hive and hbaseERROR!!!,SYSTEM EXIT!!!");
				System.exit(success);
			}		
			log.info("load index to hive and hbase SUCCESS!!!");
		}
		
		//stage 3
		if (shouldRunNextPhase(stageArgs, currentPhase)) {
			log.info("create user word vector and load to hbase START!!!");
			try{
				UserWordFeature.createUserWordFeatureFile();
				UserWordFeature.loadUserWordVectorToHbase();
			}catch(Exception se){
				log.error("create user word vector and load to hbase ERROR!!!,SYSTEM EXIT!!!");
				System.exit(success);
			}		
			log.info("create user word vector and load to hbase SUCCESS!!!");
		}
		
		//stage 4
		if (shouldRunNextPhase(stageArgs, currentPhase)) {
			log.info("prepare training data START!!!");
			try{
				ToolRunner.run(new Configuration(), new FeatureVectorizer(), new String[]{"train","noskip","20"});
			}catch(Exception se){
				log.error("prepare training data ERROR!!!,SYSTEM EXIT!!!");
				System.exit(success);
			}		
			log.info("prepare training data SUCCESS!!!");
		}
		
		//stage 5
		if (shouldRunNextPhase(stageArgs, currentPhase)) {
			log.info("prepare test data START!!!");
			try{
				ToolRunner.run(new Configuration(), new FeatureVectorizer(), new String[]{"test","noskip","20"});
			}catch(Exception se){
				log.error("prepare test data ERROR!!!,SYSTEM EXIT!!!");
				System.exit(success);
			}		
			log.info("prepare test data SUCCESS!!!");
		}
		
		HiveOperator.closeConn();
	}
	
	
	protected static boolean shouldRunNextPhase(String[] args, AtomicInteger currentPhase) {
	    int phase = currentPhase.getAndIncrement();
	    String startPhase = args[0];
	    String endPhase = args[1];
	    boolean phaseSkipped = (startPhase != null && phase < Integer.parseInt(startPhase))
	        || (endPhase != null && phase > Integer.parseInt(endPhase));
	    if (phaseSkipped) {
	      log.info("Skipping phase {}", phase);
	    }
	    return !phaseSkipped;
	  }

}
