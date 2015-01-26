package com.elex.ssp.mlctr.idx;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.mahout.common.Pair;

import com.elex.ssp.mlctr.PropertiesUtils;

public class FeatureValueEncoder {

	private static AtomicInteger sequenceId = new AtomicInteger();

	private static HashMap<String,Pair<Pair<Integer,Integer>,Integer>> countMap = new HashMap<String,Pair<Pair<Integer,Integer>,Integer>>();

	private static synchronized void setSequenceId(int start) {
		FeatureValueEncoder.sequenceId.set(start);
	}
	

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		
		processAll();
		
	}

	private static Pair<Pair<Integer,Integer>,Integer> encodeOneType(String src, String dist) throws IOException {
		int start = sequenceId.get();
		int count = 0;

		File[] files = new File(src).listFiles();

		String line;
		
		BufferedReader in;

		BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(dist),"UTF-8"));

		for (File raw : files) {
			if(!raw.getName().endsWith("crc")){
				in = new BufferedReader(new InputStreamReader(new FileInputStream(raw),"UTF-8"));
				line = in.readLine();
				while (line != null) {
					out.write(line.trim()+ ","+sequenceId.getAndIncrement()+ "\r\n");
					count++;
					line = in.readLine();
				}
				in.close();
			}
			
		}

		out.close();
		int stop = sequenceId.get();
		
		return new Pair<Pair<Integer, Integer>, Integer>(new Pair<Integer, Integer>(start,stop),count);

	}
	
	
	public static void processAll() throws IOException{
		
		setSequenceId(1);
		
		for(IdxType idx:IdxType.values()){
			
			countMap.put(idx.name(), encodeOneType(idx.getSrc(),idx.getDist()));
						
		}
				
		writeCount();
		
		merge(PropertiesUtils.getMergeFileLists());
		
	}
	
	
	private static void writeCount() throws IOException{
		BufferedWriter out = new BufferedWriter(new FileWriter(PropertiesUtils.getIdxCountPath()));
		
		for(IdxType idx:IdxType.values()){
			
			out.write("==="+idx.name()+"===\r\n");
			
			out.write("start="+countMap.get(idx.name()).getFirst().getFirst()+
					  ";stop="+countMap.get(idx.name()).getFirst().getSecond()+
					  ";count="+countMap.get(idx.name()).getSecond()+"\r\n");
		}
		
		out.close();
	}
	
	
	private static void merge(List<String> files) throws IOException{
		
		BufferedReader in;

		String line;
		BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(PropertiesUtils.getIdxMergeFilePath()),"UTF-8"));
		
		for(String file:files){
			//System.out.println(file);
			in = new BufferedReader(new InputStreamReader(new FileInputStream(file+".idx"),"UTF-8"));
			line = in.readLine();
			while (line != null) {
				out.write(line+"\r\n");
				
				line = in.readLine();
			}
			in.close();
		}
		out.close();
		
		
	}
		

}
