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
import java.util.TreeSet;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.elex.ssp.mlctr.HbaseBasis;
import com.elex.ssp.mlctr.HbaseOperator;
import com.elex.ssp.mlctr.HdfsUtil;
import com.elex.ssp.mlctr.PropertiesUtils;
import com.elex.ssp.mlctr.vector.FeatureVectorizer.SampleCounter;

public class VectorizeReducer extends Reducer<Text, Text, Text, Text> {

	private Map<String, Feature> other = new HashMap<String, Feature>();// key-idx
	//private Map<String, String> word = new HashMap<String, String>();// idx-key
	private String[] wordArr;
	private Map<String, UserDTO> user = new HashMap<String, UserDTO>();
	private MultipleOutputs<Text, Text> plain;
	private FileSystem fs;
	private HTableInterface idxTable;
	private String[] kv;
	private String unionKey;
	private Map<String,Result> resultMap = new HashMap<String,Result>();
	private int impr = 0, click = 0;
	private Text idText = new Text();
	private Text plainText = new Text();
	private StringBuffer idStr = new StringBuffer(100);
	private StringBuffer plainStr = new StringBuffer(100);
	private int wordStartIdx =0;
	private int wordArrLength=0;
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		super.setup(context);
		plain = new MultipleOutputs<Text, Text>(context);// 初始化
		fs = FileSystem.get(context.getConfiguration());
		String otherPath = PropertiesUtils.getIdxHivePath()+ "/idx_type=merge/merge.txt";
		String wordPath = PropertiesUtils.getIdxHivePath()+ "/idx_type=word/word.idx";
		loadOtherFeatureMap(fs, otherPath, other);
		//loadWordMap(fs, wordPath, word);
		loadWordArray(fs,wordPath);
		idxTable = HbaseBasis.getConn().getTable(PropertiesUtils.getIdxHbaseTable());
		wordStartIdx = HdfsUtil.readInt(new Path(PropertiesUtils.getMachineLearningRootDir()+"/word_start_idx.norm"),context.getConfiguration());
		
		System.out.println(wordStartIdx);
	}

	/**
	 * 
	 * @param fs
	 * @param src
	 * @param map

	 * @throws IOException
	 */
	/*private void loadWordMap(FileSystem fs, String src,Map<String, String> map) throws IOException {

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
	}*/
	
	private void loadWordArray(FileSystem fs, String src) throws IOException {

		wordArrLength = HdfsUtil.readInt(new Path("/ssp/mlctr/wc.norm"), fs.getConf());
		System.out.println(wordArrLength);
		wordArr = new String[wordArrLength];
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(src))));
		String line = reader.readLine();
		String[] vList;
		int index = 0 ;
		while (line != null && index<wordArrLength ) {
			vList = line.split(",");
			if (vList.length == 2) {
				wordArr[index]=vList[0];
				index++;
			}

			line = reader.readLine();
		}
		reader.close();
	}
	
	
	private void loadOtherFeatureMap(FileSystem fs, String src,Map<String, Feature> map) throws IOException {

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

		
		resultMap.clear();						
		impr = 0;
		click = 0;
				
		for (Text v : values) {
			
			kv = v.toString().split("\t");
			
			if (kv.length == 13) {
				unionKey = key.toString()+kv[2]+kv[3]+kv[4]+kv[5]+kv[6]+kv[7]+kv[8]+kv[9]+kv[10]+kv[11]+kv[12];
				
				if(resultMap.get(unionKey)==null){
					
					resultMap.put(unionKey, new Result(0,0,new TreeSet<Feature>()));
					
				}
				
				impr = kv[0] == null ? 0 : Integer.parseInt(kv[0]);
				click = kv[1] == null ? 0 : Integer.parseInt(kv[1]);
				
				resultMap.get(unionKey).setImpr(resultMap.get(unionKey).getImpr() + impr);
				resultMap.get(unionKey).setClick(resultMap.get(unionKey).getClick()+click);								
				
				//add other feature,由于同组特征可能有多条记录，因此other feature用map
				for (int i = 2; i < kv.length; i++) {
					if (other.get(kv[i]) != null)
						if(!resultMap.get(unionKey).getOtherFeature().contains(other.get(kv[i]))){
							resultMap.get(unionKey).getOtherFeature().add(other.get(kv[i]));
						}						
				}			
					
			}			
		}
		
		
				
		Iterator<Entry<String, Result>> ite = resultMap.entrySet().iterator();
		
		while(ite.hasNext()){
			
			Entry<String, Result> entry = ite.next();
			
			entry.getValue().adjustImprClick();	
			
			idStr.setLength(0);
			plainStr.setLength(0);
			
			idStr.append(entry.getValue().getImpr() + " " + entry.getValue().getClick()+ " ");
			
			plainStr.append(entry.getValue().getImpr() + " " + entry.getValue().getClick()+ " ");
			
			for (Feature f : entry.getValue().getOtherFeature()) {
				idStr.append(f.getIdx() + ":" + f.getValue() + " ");
				plainStr.append(f.getKey() + ":" + f.getValue() + " ");
			}
			
			
			if(getUserDTO(user,key.toString()) != null){
				if(getUserDTO(user,key.toString()).getIdStr() != null){
					idText.set(idStr.append(getUserDTO(user,key.toString()).getIdStr()).toString());
				}
				
				if(getUserDTO(user,key.toString()).getPlainStr() != null){
					plainText.set(plainStr.append(getUserDTO(user,key.toString()).getPlainStr()).toString());
				}
				
			}else{
				
				idText.set(idStr.toString());
				plainText.set(plainStr.toString());								
			}
			
			if(entry.getValue().getClick()==0){
				
				context.getCounter(SampleCounter.negtive).increment(1);
				
			}else{				
				context.getCounter(SampleCounter.positive).increment(1);
			}
			context.write(idText, null);
			plain.write("plain", plainText, null);			
		}
			
	}
	
	private UserDTO getUserDTO(Map<String, UserDTO> userInfoMap, String u_name) {
		
		String[] vector;
		String[] kv;
		List<Feature> list = null;
		UserDTO dto = null;
		int wordIdx=0;
		
		if(userInfoMap.get(u_name) != null){
			
			return userInfoMap.get(u_name);
			
		}else {						
			try {
				
				list = new ArrayList<Feature>();
				
				Map<String, String> result = HbaseOperator.queryOneRecord(idxTable, Bytes.toBytes(u_name));
				
				if(result.get("id")!= null ){					
					
					list.add(new Feature(u_name,result.get("id"),"1"));																				
					
				}
				
				if(result.get("vec")!=null){
					
					vector = result.get("vec").split(" ");
					
					for (String w : vector) {
						 kv = w.split(":");
						if (kv.length == 2) {
							wordIdx=Integer.parseInt(kv[0])-wordStartIdx;
							if(wordIdx < wordArrLength){
								if (wordArr[wordIdx] != null){
									list.add(new Feature(wordArr[wordIdx], kv[0],kv[1]));								
								}
							}
																							
						}

					}
				}
				
				Collections.sort(list);
				
				dto = new UserDTO(result.get("id"),u_name,list);
				
				userInfoMap.put(u_name,dto);
				
				return dto;

			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}catch(NumberFormatException ne){
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
