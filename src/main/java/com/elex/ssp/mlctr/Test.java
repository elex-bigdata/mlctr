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
	 */
	public static void main(String[] args) throws IOException, SQLException {
		
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		String hql ="INSERT OVERWRITE LOCAL DIRECTORY '/home/hadoop/wuzhongju/ctr/data/aaa' ROW format delimited FIELDS TERMINATED BY ',' stored AS textfile" +
				" select distinct concat_ws('_',source,word) from tfidf";
		stmt.execute(hql);
		System.out.println(hql);
		stmt.close();
		
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
