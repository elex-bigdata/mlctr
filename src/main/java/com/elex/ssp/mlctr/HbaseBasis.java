package com.elex.ssp.mlctr;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;


public class HbaseBasis {

    private Configuration configuration;	

	private HBaseAdmin hbaseAdmin;
	
	private HConnection connection;
	
	
	private static  HbaseBasis hbase = null;
	
	private HbaseBasis() {
		super();
		configuration = HBaseConfiguration.create();
		//configuration.set("hbase.zookeeper.quorum", "65.255.35.133");
		//configuration.set("hbase.zookeeper.property.clientPort", "1181");
		try {
			hbaseAdmin = new HBaseAdmin(configuration);
			ExecutorService pool = new ThreadPoolExecutor(5, 20, 60,TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>());
			connection = HConnectionManager.createConnection(configuration,pool);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private synchronized  static HbaseBasis getHbaseBasis(){
		if(hbase == null){
			hbase = new HbaseBasis();
		}
		return hbase;
		
	}
	
	public static HBaseAdmin getAdmin(){
		
		return getHbaseBasis().hbaseAdmin;
	}
	
	public static HConnection getConn(){
		
		return getHbaseBasis().connection;
	}
	
	public static Configuration getConf(){
		
		return getHbaseBasis().configuration;
	}
	

}
