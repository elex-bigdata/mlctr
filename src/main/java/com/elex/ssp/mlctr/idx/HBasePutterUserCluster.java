package com.elex.ssp.mlctr.idx;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.elex.ssp.mlctr.vector.FeaturePrefix;

public class HBasePutterUserCluster extends HBasePutter {	

	@Override
	public Put buildPut(String line) {
		if(line.split(",").length==2){
			Put put = new Put(Bytes.toBytes(line.split(",")[0]));
			put.add(Bytes.toBytes("idx"),Bytes.toBytes("cl"), 
					Bytes.toBytes(this.getMap().get(FeaturePrefix.cluster.getsName()+"_"+line.split(",")[1])));
			return put;
		}		
		return null;
	}

	public HBasePutterUserCluster() {
		super();
		Map<String,String> idx = new HashMap<String,String>();
		
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(IdxType.cluster.getDist()))));
			String line = reader.readLine();
			String[] kv;
			while(line != null){
				kv = line.split(",");
				if(kv.length==2){
					idx.put(kv[0], kv[1]);
				}
				line = reader.readLine();
			}
			reader.close();
			this.setMap(idx);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}


}
