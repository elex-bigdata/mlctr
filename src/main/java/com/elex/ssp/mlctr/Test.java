package com.elex.ssp.mlctr;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.mahout.common.Pair;

import com.elex.ssp.mlctr.liblinear.RandomSample;
import com.elex.ssp.mlctr.vector.Feature;
import com.elex.ssp.mlctr.vector.UserDTO;


public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		

		int count =0;
		for(int i =0;i<1000;i++){
			if(RandomUtils.nextDouble()>0.5D){
				count++;
			}			
		}
		
		System.out.println(count);
		
		
		
		
	}

}
