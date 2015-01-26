package com.elex.ssp.mlctr;


public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		for(String file:PropertiesUtils.getMergeFileLists()){
			System.out.println(file);
			
		}
	}

}
