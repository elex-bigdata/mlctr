package com.elex.ssp.mlctr.vector;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.elex.ssp.mlctr.HbaseBasis;
import com.elex.ssp.mlctr.HbaseOperator;
import com.elex.ssp.mlctr.PropertiesUtils;

public class VectorizeReducer extends Reducer<Text, Text, Text, Text> {

	private Map<String, Feature> other = new HashMap<String, Feature>();// key-idx
	private Map<String, String> word = new HashMap<String, String>();// idx-key
	private Map<String, UserDTO> user = new HashMap<String, UserDTO>();
	private MultipleOutputs<Text, Text> plain;
	private FileSystem fs;
	private HTableInterface idxTable;
	private String[] kv;
	private String unionKey;
	private Map<String,Result> result = new HashMap<String,Result>();
	private int impr = 0, click = 0;
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		super.setup(context);
		plain = new MultipleOutputs<Text, Text>(context);// 初始化mos
		fs = FileSystem.get(context.getConfiguration());
		String otherPath = PropertiesUtils.getIdxHivePath()+ "/idx_type=merge/merge.txt";
		String wordPath = PropertiesUtils.getIdxHivePath()+ "/idx_type=word/word.idx";
		readOtherFeature(fs, otherPath, other);
		readIdxMap(fs, wordPath, word);
		idxTable = HbaseBasis.getConn().getTable(PropertiesUtils.getIdxHbaseTable());
	}

	/**
	 * 
	 * @param fs
	 * @param src
	 * @param map

	 * @throws IOException
	 */
	private void readIdxMap(FileSystem fs, String src,Map<String, String> map) throws IOException {

		BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(src))));
		String line = reader.readLine();
		while (line != null) {
			String[] vList = line.split(",");
			if (vList.length == 2) {
				map.put(vList[1], vList[0]);
			}

			line = reader.readLine();
		}
		reader.close();
	}
	
	
	private void readOtherFeature(FileSystem fs, String src,Map<String, Feature> map) throws IOException {

		BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(src))));
		String line = reader.readLine();
		while (line != null) {
			String[] vList = line.split(",");
			if (vList.length == 2) {
				map.put(vList[0],new Feature( vList[0],vList[1],"1"));
			}

			line = reader.readLine();
		}
		reader.close();
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		
		result.clear();						
		impr = 0;
		click = 0;
				
		for (Text v : values) {
			
			kv = v.toString().split("\t");
			
			if (kv.length == 13) {
				unionKey = key.toString()+kv[2]+kv[3]+kv[4]+kv[5]+kv[6]+kv[7]+kv[8]+kv[9]+kv[10]+kv[11]+kv[12];
				
				if(result.get(unionKey)==null){
					
					result.put(unionKey, new Result(0,0,new ArrayList<Feature>()));
					
				}
				
				impr = kv[0] == null ? 0 : Integer.parseInt(kv[0]);
				click = kv[1] == null ? 0 : Integer.parseInt(kv[1]);
				
				result.get(unionKey).setImpr(result.get(unionKey).getImpr() + impr);
				result.get(unionKey).setClick(result.get(unionKey).getClick()+click);
				
				//add user feature
				if (getUserIdx(user, key.toString()) != null) {
					result.get(unionKey).getfList().addAll(getUserIdx(user, key.toString()).getWordList());
				}
				
				//add other feature
				for (int i = 2; i < kv.length; i++) {
					if (other.get(kv[i]) != null)
						result.get(unionKey).getfList().add(other.get(kv[i]));
				}			
					
			}			
		}
				
		Iterator<Entry<String, Result>> ite = result.entrySet().iterator();
		
		while(ite.hasNext()){
			
			Entry<String, Result> entry = ite.next();
			
			Collections.sort(entry.getValue().getfList());
			
			entry.getValue().adjustImprClick();
			
			StringBuffer idStr = new StringBuffer(100);
			StringBuffer plainStr = new StringBuffer(100);
			
			idStr.append(entry.getValue().getImpr() + " " + entry.getValue().getClick()+ " ");
			
			plainStr.append(entry.getValue().getImpr() + " " + entry.getValue().getClick()+ " ");
			
			for (Feature f : entry.getValue().getfList()) {
				idStr.append(f.getIdx() + ":" + f.getValue() + " ");
				plainStr.append(f.getKey() + ":" + f.getValue() + " ");
			}

			context.write(new Text(idStr.toString()), null);
			plain.write("plain", new Text(plainStr.toString()), null);
		}
	

		
	}
	

	private UserDTO getUserIdx(Map<String, UserDTO> userInfoMap, String uid) {
		
		String[] vector;
		String[] kv;
		List<Feature> list = null;
		UserDTO dto = null;
		
		if(userInfoMap.get(uid) != null){
			
			return userInfoMap.get(uid);
			
		}else {						
			try {
				
				list = new ArrayList<Feature>();
				
				Map<String, String> result = HbaseOperator.queryOneRecord(idxTable, Bytes.toBytes(uid));
				
				if(result.get("id")!= null ){					
					
					list.add(new Feature(uid,result.get("id"),"1"));																				
					
				}
				
				if(result.get("vec")!=null){
					
					vector = result.get("vec").split(" ");
					
					for (String w : vector) {
						 kv = w.split(":");
						if (kv.length == 2) {
							if (word.get(kv[0]) != null)
								list.add(new Feature(word.get(kv[0]), kv[0],kv[1]));
						}

					}
				}
				
				dto = new UserDTO(result.get("id"),result.get("vec"),list);
				
				userInfoMap.put(uid,dto);
				
				return dto;

			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
		}

	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		super.cleanup(context);
		plain.close();// 释放资源
		idxTable.close();
	}

}
