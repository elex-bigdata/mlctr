package com.elex.ssp.mlctr;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.Pair;

import com.elex.ssp.mlctr.idx.HBasePutter;
import com.elex.ssp.mlctr.idx.IdxType;
import com.elex.ssp.mlctr.liblinear.RandomSample;
import com.elex.ssp.mlctr.vector.Feature;
import com.elex.ssp.mlctr.vector.UserDTO;
import com.google.common.io.Closeables;


public class Test {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException {
		
		for(int i=0;i<3;i++){
			Class onwClass1 = Class.forName("com.elex.ssp.mlctr.idx.HBasePutterMergeIdx");
			HBasePutter putter1 = (HBasePutter) onwClass1.newInstance();
			System.out.println(putter1.toString());
		}
		  		
	}
	
	private static void loadWordMap(String src,Map<String, String> map) throws IOException {

		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(src))));
		String line = reader.readLine();
		String[] vList;
		int count = 0;
		while (line != null && count <1000000) {
			vList = line.split(",");
			if (vList.length == 2) {
				map.put(vList[1], vList[0]);
			}

			line = reader.readLine();
		}
		reader.close();
	}

	private static void loadWordArray(String src,String[] arr) throws IOException {

		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(src))));
		String line = reader.readLine();
		String[] vList;
		int index = 0 ;
		while (line != null) {
			vList = line.split(",");
			if (vList.length == 2) {
				arr[index]=vList[0];
				index++;
			}

			line = reader.readLine();
		}
		reader.close();
	}
	
	public static int readInt(String path) throws IOException {
	   DataInputStream in = new DataInputStream(new FileInputStream(new File(path)));
	    try {
	      return in.readInt();
	    } finally {
	      Closeables.closeQuietly(in);
	    }
	  }
}
