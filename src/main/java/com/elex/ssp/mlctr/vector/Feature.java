package com.elex.ssp.mlctr.vector;

import java.io.Serializable;

public class Feature implements Serializable,Comparable<Feature> {
	
	private String key;
	private String idx;
	private String value;
	
	public Feature(String key, String idx, String value) {
		super();
		this.key = key;
		this.idx = idx;
		this.value = value;
	}

	
	
	public String getKey() {
		return key;
	}


	public void setKey(String key) {
		this.key = key;
	}

	public String getIdx() {
		return idx;
	}


	public void setIdx(String idx) {
		this.idx = idx;
	}


	public String getValue() {
		return value;
	}


	public void setValue(String value) {
		this.value = value;
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -7595652363869377418L;


	/**
	 * @param args
	 */
	public static void main(String[] args) {


	}


	@Override
	public int compareTo(Feature o) {
		return Integer.parseInt(this.idx)-Integer.parseInt(o.getIdx());
		
	}


	

}
