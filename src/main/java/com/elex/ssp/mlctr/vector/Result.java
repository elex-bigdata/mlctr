package com.elex.ssp.mlctr.vector;

import java.io.Serializable;
import java.util.List;

public class Result implements Serializable {
	
	private int impr;
	private int click;
	private List<Feature> fList;
	
	public Result(int impr, int click, List<Feature> fList) {
		super();
		this.impr = impr;
		this.click = click;
		this.fList = fList;
	}

	public int getImpr() {
		return impr;
	}

	public void setImpr(int impr) {
		this.impr = impr;
	}

	public int getClick() {
		return click;
	}

	public void setClick(int click) {
		this.click = click;
	}

	public List<Feature> getfList() {
		return fList;
	}

	public void setfList(List<Feature> fList) {
		this.fList = fList;
	}

	public void adjustImprClick(){
		
		if (this.click >= this.impr) {
			
			this.click = this.impr;
		}
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -7063450325286246567L;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
