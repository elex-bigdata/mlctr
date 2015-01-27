package com.elex.ssp.mlctr.vector;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.Pair;

import com.elex.ssp.TimeUtils;
import com.elex.ssp.mlctr.Constants;
import com.elex.ssp.mlctr.HbaseBasis;
import com.elex.ssp.mlctr.HbaseOperator;
import com.elex.ssp.mlctr.HdfsUtil;
import com.elex.ssp.mlctr.HiveOperator;
import com.elex.ssp.mlctr.PropertiesUtils;

public class FeatureVectorizer extends Configured implements Tool {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new FeatureVectorizer(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		Job job = Job.getInstance(conf, "FeatureVectorizer");
		job.setJarByClass(FeatureVectorizer.class);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);

		if(args[0].equals("train")){
			String inPath= PropertiesUtils.getMachineLearningRootDir()+ "/train/input";						
			String hql = "INSERT OVERWRITE DIRECTORY '"+ inPath + "'"
					+ " select uid,pid,ip,time,nation,ua,os,adid,ref,opt,impr,click from log_merge where uid is not null and day>'"
					+ Constants.getLastNDay(PropertiesUtils.getTrainDays()) + "'"
					+ " and day <'"+Constants.getLastNDay(5)+"' and array_contains(array("
					+ PropertiesUtils.getNations() + "),nation) and adid like '5%'";
			
			HiveOperator.executeHQL(hql);
			System.out.println(hql);
			Path in = new Path(inPath);
			FileInputFormat.addInputPath(job, in);
			
			job.setOutputFormatClass(TextOutputFormat.class);
			MultipleOutputs.addNamedOutput(job, "plain", TextOutputFormat.class, Text.class, Text.class);
			String output = PropertiesUtils.getMachineLearningRootDir() + "/train/output";
			HdfsUtil.delFile(fs, output);
			FileOutputFormat.setOutputPath(job, new Path(output));
			
			
		}else if(args[0].equals("test")){
			String inPath = PropertiesUtils.getMachineLearningRootDir()+ "/test/input";
			String hql = "INSERT OVERWRITE DIRECTORY '"+ inPath + "'"
					+ " select uid,pid,ip,time,nation,ua,os,adid,ref,opt,impr,click" 
					+ " from log_merge where uid is not null and  day>'"+ Constants.getLastNDay(5) + "'"
					+ " and array_contains(array("+ PropertiesUtils.getNations() + "),nation) and adid like '5%'";
			
			HiveOperator.executeHQL(hql);
			System.out.println(hql);
			Path in = new Path(inPath);
			FileInputFormat.addInputPath(job, in);
			
			job.setOutputFormatClass(TextOutputFormat.class);
			MultipleOutputs.addNamedOutput(job, "plain", TextOutputFormat.class, Text.class, Text.class);
			String output = PropertiesUtils.getMachineLearningRootDir() + "/test/output";
			HdfsUtil.delFile(fs, output);
			FileOutputFormat.setOutputPath(job, new Path(output));
			
		}else{
			System.err.println("FeatureVectorizer wrong arg value!!!"+args[0]);
		}
						
		return job.waitForCompletion(true) ? 0 : 1;
	}

	// TextOutputFormat的输出文件key为long的字节偏移量
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		private String[] values;
		private String newKey,newVal,t;
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			values = value.toString().split("\\x01");
			
			//uid,pid,ip,time,nation,ua,os,adid,ref,opt,impr,click
			
			try {
				String[] tF = TimeUtils.getTimeDimension(new String[]{values[3],""});
				for(String td:tF){
					t=t+ FeaturePrefix.time.getsName()+"_"+td+"/t";
				}
				
				newKey = FeaturePrefix.user.getsName()+"_"+values[0]+"/t"
						   + FeaturePrefix.project.getsName()+"_"+values[1]+"/t"
						   + FeaturePrefix.area.getsName()+"_"+Constants.getArea(values[2])+"/t"
						   + t
						   + FeaturePrefix.nation.getsName()+"_"+values[4]+"/t"
						   + FeaturePrefix.browser.getsName()+"_"+values[5]+"/t"
						   + FeaturePrefix.os.getsName()+"_"+values[6]+"/t"
						   + FeaturePrefix.adid.getsName()+"_"+values[7]+"/t"
						   + FeaturePrefix.ref.getsName()+"_"+values[8]+"/t"
						   + FeaturePrefix.opt.getsName()+"_"+values[9];
					newVal = values[10]==null?"0":values[10]+"/t"+values[11]==null?"0":values[11];
					
					context.write(new Text(newKey), new Text(newVal));
			} catch (ParseException e) {
				e.printStackTrace();
			}
			
			
		}
	
		
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		
		private Map<String,String> other = new HashMap<String,String>();//key-idx
		private Map<String,String> word = new HashMap<String,String>();//idx-key
		private Map<String,Pair<String,String>> user = new HashMap<String,Pair<String,String>>();//uid,<idx,wordvector>
		private MultipleOutputs<Text, Text> plain; 
		private FileSystem fs;
		private HTableInterface idxTable; 
		private String[] kv;
		

		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			plain = new MultipleOutputs<Text, Text>(context);// 初始化mos			
		    fs = FileSystem.get(context.getConfiguration());
		    String otherPath = PropertiesUtils.getIdxHivePath()+"/idx_type=merge/merge.txt";
		    String wordPath = PropertiesUtils.getIdxHivePath()+"/idx_type=word/word.idx";
		    readIdxMap(fs,otherPath,other,false);
		    readIdxMap(fs,wordPath,word,true);
		    idxTable = HbaseBasis.getConn().getTable(PropertiesUtils.getIdxHbaseTable());
		}


		private void readIdxMap(FileSystem fs,String src,Map<String,String> map,boolean switch_kv) throws IOException {
			
			BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(src)))); 
			String line =reader.readLine();
	        while(line != null){
	        	String[] vList = line.split(",");
	        	if(vList.length==2){
	        		if(switch_kv){
	        			map.put(vList[1],vList[0]);
	        		}else{
	        			map.put(vList[0],vList[1]);
	        		}
	        		
	        	}
	        	
	        	line = reader.readLine();
	        }
	        reader.close();
		}


		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			StringBuffer idStr = new StringBuffer(100);
			StringBuffer plainStr = new StringBuffer(100);
			
			List<Feature> fList = new ArrayList<Feature>();
			int impr=0,click=0;
			for(Text v:values){
				kv = v.toString().split("\t");
				impr = impr+kv[0]==null?0:Integer.parseInt(kv[0]);
				click = click+kv[1]==null?0:Integer.parseInt(kv[1]);
			}
			
			if(click>=impr){
				click = impr;
			}
			
			kv = key.toString().split("\t");
			
			if(getUserIdx(user,kv[0])!=null) fList.add(new Feature(kv[0],getUserIdx(user,kv[0]),"1"));
			
			for(int i=1;i<kv.length;i++){
				if(other.get(kv[i])!=null) fList.add(new Feature(kv[i],other.get(kv[i]),"1"));
			}
			
			getUserWordVector(user,kv[0],fList);
			
			Collections.sort(fList);
			
			idStr.append(impr+" "+click+" ");
			plainStr.append(impr+" "+click+" ");
			for(Feature f:fList){
				idStr.append(f.getIdx()+":"+f.getValue()+" ");
				plainStr.append(f.getKey()+":"+f.getValue()+" ");
			}
				
			context.write(new Text(idStr.toString()), null);
			plain.write("plain",new Text(plainStr.toString()), null);
		}
		
		private void getUserWordVector(Map<String, Pair<String, String>> userInfoMap, String uid,List<Feature> list) {
			
			String[] vector = userInfoMap.get(uid).getSecond().split(" ");
			for(String w:vector){
				String[] kv = w.split(":");
				if(kv.length==2){					
					if(word.get(kv[0])!=null) list.add(new Feature(word.get(kv[0]),kv[0],kv[1]));
				}
				
			}
		}


		private String getUserIdx(Map<String, Pair<String, String>> userInfoMap,String uid) {
			String idx;
			idx = userInfoMap.get(uid).getFirst();
			if(idx != null){
				return idx;
			}else{
				try {
					Map<String,String> result = HbaseOperator.queryOneRecord(idxTable, Bytes.toBytes(uid));
					if(userInfoMap.size()<1000000){
						userInfoMap.put(uid,new Pair<String, String>(result.get("idx"),result.get("vec")));
					}					
					return result.get("idx");
					
				} catch (IOException e) {
					e.printStackTrace();
					return null;
				}
			}
			
		}


		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			super.cleanup(context);
			plain.close();// 释放资源
			idxTable.close();
		}
		
	}
}
