package com.elex.ssp.mlctr.liblinear;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;


public class Test20news {

	/**
	 * @param args
	 * @throws NoSuchMethodException 
	 * @throws SecurityException 
	 * @throws InvocationTargetException 
	 * @throws IllegalAccessException 
	 * @throws IllegalArgumentException 
	 */
	public static void main(String[] args) throws SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException {
		
		//train
		Method train = de.bwaldvogel.liblinear.Train.class.getMethod("main",String[].class); 
		train.invoke(null,(Object)new String[] {"-s","2","D:\\20news-bydate\\news20","D:\\20news-bydate\\news20-s2-java.mode"});
		
		//test
		Method predict = de.bwaldvogel.liblinear.Predict.class.getMethod("main",String[].class); 
		predict.invoke(null,(Object)new String[] {"D:\\20news-bydate\\news20.t","D:\\20news-bydate\\news20-s2-java.mode","liblinear-news20-java.out"});
		
	}

}
