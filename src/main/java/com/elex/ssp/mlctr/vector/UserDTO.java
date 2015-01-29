package com.elex.ssp.mlctr.vector;

import java.io.Serializable;
import java.util.List;

public class UserDTO implements Serializable {
	
	private String id;//索引号
	private String u_name;//"u_"打头的用户名
	private List<Feature> wordList;//包括该用户的ssp、gdp、odp特征和uid特征
	

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
