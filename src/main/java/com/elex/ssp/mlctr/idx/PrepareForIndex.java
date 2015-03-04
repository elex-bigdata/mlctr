package com.elex.ssp.mlctr.idx;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import com.elex.ssp.mlctr.Constants;
import com.elex.ssp.mlctr.HiveOperator;
import com.elex.ssp.mlctr.PropertiesUtils;

public class PrepareForIndex {
	
	

	/**
	 * @param args
	 * @throws SQLException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws SQLException, IOException {
		prepareAll();
	}
	
	
	public static void prepareAll() throws SQLException, IOException{
		//getDistinctUserIdList();
		//getDistinctWordList();
		getDistinctOdpList();
		getDistinctOtherList();
		getDistinctNationList(PropertiesUtils.isAllAdid());
		getDistinctOsList();
		getDistinctAdidList(PropertiesUtils.isAllNation());
	}
	
	public static void getDistinctAdidList(boolean isAll) throws SQLException {

		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		String hql ="INSERT OVERWRITE LOCAL DIRECTORY '"+IdxType.adid.getSrc()+"' ROW format delimited FIELDS TERMINATED BY ',' stored AS textfile" +
				    " select distinct concat_ws('_','ad',adid) from feature_merge where ft='project'";
		if(isAll){
			stmt.execute(hql);
		}else{
			hql=hql+" and adid like '5%'";
			stmt.execute(hql);
		}
		
		System.out.println(hql);
		stmt.close();
		
	}
	
	public static void getDistinctOdpList() throws SQLException {

		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		String hql ="INSERT OVERWRITE LOCAL DIRECTORY '"+IdxType.odp.getSrc()+"' ROW format delimited FIELDS TERMINATED BY ',' stored AS textfile" +
				    " select distinct concat_ws('_','odp',tag) from user_tag";
		stmt.execute(hql);
		System.out.println(hql);
		stmt.close();
		
	}

	/*public static void getDistinctUserIdList() throws SQLException {

		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		String preHql ="INSERT OVERWRITE LOCAL DIRECTORY '"+IdxType.user.getSrc()+"' ROW format delimited FIELDS TERMINATED BY ',' stored AS textfile";
		
		String hql;
		
		if(PropertiesUtils.getPruneUserByImpr()==0){
			hql = preHql +" select distinct concat_ws('_','u',fv) from feature_merge where ft ='user'";
		}else{
			hql = preHql +" select uid from(select concat_ws('_','u',fv) as uid,sum(impr) as s_i from feature_merge" +
					" where ft='user' and array_contains (array ("+PropertiesUtils.getNations()+"), nation) group by concat_ws('_','u',fv))a" +
					" where a.s_i>"+PropertiesUtils.getPruneUserByImpr();
		}
		stmt.execute(hql);
		System.out.println(hql);
		stmt.close();
		
	}*/

	/*public static void getDistinctWordList() throws SQLException {

		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		String preHql = "INSERT OVERWRITE LOCAL DIRECTORY '"+IdxType.word.getSrc()+"' ROW format delimited FIELDS TERMINATED BY ',' stored AS textfile" ;
		String hql;
		if(PropertiesUtils.getPruneWordByWc()==0){
			hql = preHql +" select distinct concat_ws('_',source,word) from tfidf";
		}else{
			hql = preHql +" select fv from (select concat_ws('_',source,word) as fv,count(1) as rc from tfidf group by concat_ws('_',source,word))a where a.rc>"+PropertiesUtils.getPruneWordByWc();
		}		
		stmt.execute(hql);
		System.out.println(hql);
		stmt.close();
		
	}*/

	public static void getDistinctOtherList() throws SQLException {

		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		String hql ="INSERT OVERWRITE LOCAL DIRECTORY '"+IdxType.other.getSrc()+"' ROW format delimited FIELDS TERMINATED BY ',' stored AS textfile" +
				    " select distinct concat_ws('_',case ft when 'browser' then 'ua'" +
				    " when 'project' then 'p' when 'time' then 't'" +
				    " when 'area' then 'ip' else ft end,fv) from feature_merge where ft !='keyword' and ft !='user' and ft not like 'query%'";
		stmt.execute(hql);
		System.out.println(hql);
		stmt.close();
		
	}

	public static void getDistinctNationList(boolean isAll) throws SQLException, IOException {

		if(isAll){
			Connection con = HiveOperator.getHiveConnection();
			Statement stmt = con.createStatement();
			String hql ="INSERT OVERWRITE LOCAL DIRECTORY '"+IdxType.nation.getSrc()+"' ROW format delimited FIELDS TERMINATED BY ',' stored AS textfile" +
					" select distinct concat_ws('_','na',nation) from feature_merge where ft='project'";
			stmt.execute(hql);
			System.out.println(hql);
			stmt.close();
		}else{
			BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(IdxType.nation.getSrc()+"/nation.txt"),"UTF-8"));
			String[] nations = PropertiesUtils.getNations().replace("'", "").split(",");
			for(String na:nations){
				out.write("na_"+na+"\r\n");
			}
			out.close();
		}
		
	}

	public static void getDistinctOsList() throws SQLException {

		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		String hql ="INSERT OVERWRITE LOCAL DIRECTORY '"+IdxType.os.getSrc()+"' ROW format delimited FIELDS TERMINATED BY ',' stored AS textfile" +
				" select distinct concat_ws('_','os',os) from log_merge where day>'"+Constants.getLastNDay(5)+"'";
		stmt.execute(hql);
		System.out.println(hql);
		stmt.close();
	}
}
