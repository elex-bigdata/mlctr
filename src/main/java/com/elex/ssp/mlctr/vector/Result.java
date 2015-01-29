package com.elex.ssp.mlctr.vector;

import java.io.Serializable;
import java.util.Set;


public class Result implements Serializable {
	
	private int impr;
	private int click;
	private Set<Feature> otherFeature;//存放time、area、project等特征
			
	
	public Result(int impr, int click,Set<Feature> other) {
		super();
		this.impr = impr;
		this.click = click;
		this.otherFeature = other;
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
	
	public Set<Feature> getOtherFeature() {
		return otherFeature;
	}

	public void setOtherFeature(Set<Feature> otherFeature) {
		this.otherFeature = otherFeature;
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
