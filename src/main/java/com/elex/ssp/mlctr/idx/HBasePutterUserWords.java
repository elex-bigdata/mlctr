package com.elex.ssp.mlctr.idx;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class HBasePutterUserWords extends HBasePutter {	

	@Override
	public Put buildPut(String line) {
		if(line.split(",").length==2){
			Put put = new Put(Bytes.toBytes(line.split(",")[0]));
			put.add(Bytes.toBytes("idx"),Bytes.toBytes("vec"), Bytes.toBytes(line.split(",")[1]));
			return put;
		}		
		return null;
	}

	private HBasePutterUserWords() {
		super();
	}


}