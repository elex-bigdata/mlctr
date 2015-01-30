package com.elex.ssp.mlctr;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

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
		

		
		String a = "1 1 1:1 3:1 46:1 1228:1 1744:1 3444:1 3450:1 3452:1 61923:0.366 740861:0.634 7219248:1";
		
		RandomSample rs = new RandomSample();
		
		System.out.println(rs.isPositive(a));
		
		
		
		
	}

}
