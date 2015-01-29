package com.elex.ssp.mlctr;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.mahout.common.Pair;

import com.elex.ssp.mlctr.vector.Feature;


public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Feature f1 = new Feature("a","8","1");
		Feature f2 = new Feature("a","30","2");
		Feature f3 = new Feature("a","12","3");
		Feature f4 = new Feature("a","90","4");
		
		Set<Feature> set = new TreeSet<Feature>();
		set.add(f3);
		set.add(f1);
		set.add(f4);
		set.add(f2);
		
		for(Feature f:set){
		
			System.out.println(f.getIdx());
		}
		
		
		
		
	}

}
