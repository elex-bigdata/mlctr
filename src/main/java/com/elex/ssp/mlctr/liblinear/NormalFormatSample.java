package com.elex.ssp.mlctr.liblinear;

import java.io.IOException;

public class NormalFormatSample extends RandomSample {

	public boolean isPositive(String line){
		if(line != null){
			if(line.trim().startsWith("+")){			
				return true;
			}
		}
			
		return false;
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		
		NormalFormatSample sam = new NormalFormatSample();
		
		sam.setSampleRatio(Double.parseDouble(args[0]));
		
		sam.setNegtiveFilePath(args[1]);
		
		sam.setOutFilePath(args[2]);
		
		sam.sampling();
		
	}

}
