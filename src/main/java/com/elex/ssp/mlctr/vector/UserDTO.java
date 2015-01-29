package com.elex.ssp.mlctr.vector;

import java.io.Serializable;
import java.util.List;

public class UserDTO implements Serializable {
	
	private String id;//索引号
	private String u_name;//"u_"打头的用户名
	private List<Feature> wordList;//包括该用户的ssp、gdp、odp特征和uid特征
	private String idStr;		
	private String plainStr;
	

	public UserDTO(String id, String u_name, List<Feature> wordList) {
		super();
		this.id = id;
		this.u_name = u_name;
		this.wordList = wordList;
	}

	public String getU_name() {
		return u_name;
	}

	public void setU_name(String u_name) {
		this.u_name = u_name;
	}
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}


	public List<Feature> getWordList() {
		return wordList;
	}

	public void setWordList(List<Feature> wordList) {
		this.wordList = wordList;
	}
	
	public String getIdStr() {
		if(idStr != null){
			return idStr;
		}else{
			createOutStr();
			return idStr;
		}
		
	}

	public void setIdStr(String idStr) {
		this.idStr = idStr;
	}

	public String getPlainStr() {
		
		if(plainStr != null){
			return plainStr;
		}else{
			createOutStr();
			return plainStr;
		}
	}

	public void setPlainStr(String plainStr) {
		this.plainStr = plainStr;
	}
	
	private void createOutStr(){
				
		if(this.wordList != null){		
			
			StringBuffer idStr = new StringBuffer(100);
			StringBuffer plainStr = new StringBuffer(100);
			for (Feature f : this.wordList) {
				idStr.append(f.getIdx() + ":" + f.getValue() + " ");
				plainStr.append(f.getKey() + ":" + f.getValue() + " ");
			}
			this.setIdStr(idStr.toString());
			this.setPlainStr(plainStr.toString());
		}
		
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -1191077759689407567L;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
