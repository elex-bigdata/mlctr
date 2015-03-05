package com.elex.ssp.mlctr.idx;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;

import com.elex.ssp.mlctr.HbaseBasis;


public abstract class HBasePutter implements Callable<String> {

	private String tableName;
	private List<String> lines;
	private String file;
	private Map<String,String> map;
	
	
	public String getTableName() {
		return tableName;
	}


	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public Map<String, String> getMap() {
		return map;
	}


	public void setMap(Map<String, String> map) {
		this.map = map;
	}



	public List<String> getLines() {
		return lines;
	}


	public void setLines(List<String> lines) {
		this.lines = lines;
	}


	public String getFile() {
		return file;
	}


	public void setFile(String file) {
		this.file = file;
	}


	

	@Override
	public String call() throws Exception {
		HTableInterface  table = HbaseBasis.getConn().getTable(tableName);
		List<Put> puts = new ArrayList<Put>();
		long begin = System.currentTimeMillis();
		for (String line : lines) {
			try {
				if(buildPut(line) != null){
					puts.add(buildPut(line));
				}
				
				
			} catch (Exception e) {
				System.err.println("get exception:" + e.getMessage()+ ", ignore the log : " + line);
			}
		}
		if (puts.size() > 0) {
			try {
				table.setAutoFlush(false);
				table.batch(puts);		
			} catch (Exception e) {
				// 此时的失败一般是因为HBase出问题了，暂不分离日志
				System.err.println("Fail to insert data after spend "+ (System.currentTimeMillis() - begin) + "ms as ："+ e.getMessage());
				throw e;
			} finally {
				table.flushCommits();
				table.close();
			}
		}
		return puts.size()+"("+file+")";
	}	
	
	
	public abstract  Put buildPut(String line); 
	
	
}
