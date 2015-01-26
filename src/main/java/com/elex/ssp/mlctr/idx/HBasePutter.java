package com.elex.ssp.mlctr.idx;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;


public class HBasePutter implements Callable<String> {

	private HTableInterface table;
	private List<String> lines;
	private String file;

	public HBasePutter(HTableInterface tableName, List<String> lines,
			String file) {
		super();
		this.table = tableName;
		this.lines = lines;
		this.file = file;
	}

	@Override
	public String call() throws Exception {
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

	
	
	private Put buildPut(String line) {
		if(line.split(",").length==2){
			Put put = new Put(Bytes.toBytes(line.split(",")[0]));
			put.add(Bytes.toBytes("idx"),Bytes.toBytes("id"), Bytes.toBytes(line.split(",")[1]));
			return put;
		}		
		return null;
						
	}
}
