package com.elex.ssp.mlctr.liblinear;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.commons.lang.math.RandomUtils;

public class RandomSample {
	
	private double sampleRatio;
	private String negtiveFilePath;
	private String outFilePath;
	
		
	public String getOutFilePath() {
		return outFilePath;
	}

	public void setOutFilePath(String outFilePath) {
		this.outFilePath = outFilePath;
	}

	public String getNegtiveFilePath() {
		return negtiveFilePath;
	}

	public void setNegtiveFilePath(String negtiveFilePath) {
		this.negtiveFilePath = negtiveFilePath;
	}

	public double getSampleRatio() {
		return sampleRatio;
	}

	public void setSampleRatio(double sampleRatio) {
		this.sampleRatio = sampleRatio;
	}

	private void sampling() throws IOException{
		
		BufferedReader in;

		String line;
		BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outFilePath),"UTF-8"));
		in = new BufferedReader(new InputStreamReader(new FileInputStream(negtiveFilePath),"UTF-8"));
		line = in.readLine();
		
		while (line != null) {
			if(RandomUtils.nextDouble() > sampleRatio || isPositive(line) ){
				out.write(line+"\r\n");
			}
			
			line = in.readLine();
		}
		
		in.close();
		out.close();
		
	}
	
	public boolean isPositive(String line){
		try{
			if(Integer.parseInt(line.substring(line.indexOf(" ")+1, line.indexOf(" ", line.indexOf(" ")+1)))>0){
				
				return true;
			}
		}catch(NumberFormatException ne){
			return false;
		}				
		return false;
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		
		RandomSample sam = new RandomSample();
		
		sam.setSampleRatio(Double.parseDouble(args[0]));
		
		sam.setNegtiveFilePath(args[1]);
		
		sam.setOutFilePath(args[2]);
		
		sam.sampling();
		
	}

}
