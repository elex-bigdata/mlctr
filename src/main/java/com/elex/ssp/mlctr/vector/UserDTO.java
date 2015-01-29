package com.elex.ssp.mlctr.vector;

import java.io.Serializable;
import java.util.List;

public class UserDTO implements Serializable {
	
	private String id;//索引号
	private String wordIdVector;//索引的词频向量
	private List<Feature> wordList;//明文的词频向量
	

	public UserDTO(String id, String wordIdVector, List<Feature> wordList) {
		super();
		this.id = id;
		this.wordIdVector = wordIdVector;
		this.wordList = wordList;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getWordIdVector() {
		return wordIdVector;
	}

	public void setWordIdVector(String wordIdVector) {
		this.wordIdVector = wordIdVector;
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
