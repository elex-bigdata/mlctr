package com.elex.ssp.mlctr;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
			log.info("create tables!!!");
			//success = createTables();
			if (success != 0) {
				log.error("create tables ERROR!!!,SYSTEM EXIT!!!");
				System.exit(success);
			}
			log.info("create tables SUCCESS!!!");
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
