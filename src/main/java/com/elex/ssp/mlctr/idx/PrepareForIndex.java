package com.elex.ssp.mlctr.idx;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import com.elex.ssp.mlctr.Constants;
import com.elex.ssp.mlctr.HiveOperator;

public class PrepareForIndex {

	/**
	 * @param args
	 * @throws SQLException 
	 */
	public static void main(String[] args) throws SQLException {
		prepareAll();
	}
	
	
	public static void prepareAll() throws SQLException{
		getDistinctUserIdList();
		getDistinctWordList();
		getDistinctOtherList();
		getDistinctNationList();
		getDistinctOsList();
	}

	public static void getDistinctUserIdList() throws SQLException {

		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		String hql ="INSERT OVERWRITE LOCAL DIRECTORY '"+IdxType.user.getSrc()+"' ROW format delimited FIELDS TERMINATED BY ',' stored AS textfile" +
			    " select distinct concat_ws('_',ft,fv) from feature_merge where ft ='user'";
		stmt.execute(hql);
		stmt.close();
		
	}

	public static void getDistinctWordList() throws SQLException {

		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		String hql ="INSERT OVERWRITE LOCAL DIRECTORY '"+IdxType.word.getSrc()+"' ROW format delimited FIELDS TERMINATED BY ',' stored AS textfile" +
				    " select distinct concat_ws('_',source,word) from tfidf";
		stmt.execute(hql);
		System.out.println(hql);
		stmt.close();
		
	}

	public static void getDistinctOtherList() throws SQLException {

		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		String hql ="INSERT OVERWRITE LOCAL DIRECTORY '"+IdxType.other.getSrc()+"' ROW format delimited FIELDS TERMINATED BY ',' stored AS textfile" +
				    " select distinct concat_ws('_',ft,fv) from feature_merge where ft !='keyword' and ft !='user' and ft not like 'query%'";
		stmt.execute(hql);
		System.out.println(hql);
		stmt.close();
		
	}

	public static void getDistinctNationList() throws SQLException {

		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		String hql ="INSERT OVERWRITE LOCAL DIRECTORY '"+IdxType.nation.getSrc()+"' ROW format delimited FIELDS TERMINATED BY ',' stored AS textfile" +
				" select distinct concat_ws('_','nation',nation) from log_merge where day='"+Constants.getLastFiveDay()+"'";
		stmt.execute(hql);
		System.out.println(hql);
		stmt.close();
	}

	public static void getDistinctOsList() throws SQLException {

		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		String hql ="INSERT OVERWRITE LOCAL DIRECTORY '"+IdxType.os.getSrc()+"' ROW format delimited FIELDS TERMINATED BY ',' stored AS textfile" +
				" select distinct concat_ws('_','os',os) from log_merge where day='"+Constants.getLastFiveDay()+"'";
		stmt.execute(hql);
		System.out.println(hql);
		stmt.close();
	}
}
