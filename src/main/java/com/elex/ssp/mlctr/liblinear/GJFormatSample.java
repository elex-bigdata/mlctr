package com.elex.ssp.mlctr.liblinear;

import java.io.IOException;

public class GJFormatSample extends RandomSample {

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
		
		GJFormatSample sam = new GJFormatSample();
		
		sam.setSampleRatio(Double.parseDouble(args[0]));
		
		sam.setNegtiveFilePath(args[1]);
		
		sam.setOutFilePath(args[2]);
		
		sam.sampling();
		
	}

}
