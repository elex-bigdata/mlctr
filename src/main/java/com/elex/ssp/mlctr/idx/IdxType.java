package com.elex.ssp.mlctr.idx;

import com.elex.ssp.mlctr.Constants;

public enum IdxType {
	
	nation(Constants.IDX_PATH+"/nation",Constants.IDX_PATH+"/nation.idx"),
	os(Constants.IDX_PATH+"/os",Constants.IDX_PATH+"/os.idx"),
	adid(Constants.IDX_PATH+"/adid",Constants.IDX_PATH+"/adid.idx"),
	other(Constants.IDX_PATH+"/other",Constants.IDX_PATH+"/other.idx"),
	word(Constants.IDX_PATH+"/word",Constants.IDX_PATH+"/word.idx"),
	user(Constants.IDX_PATH+"/user",Constants.IDX_PATH+"/user.idx");
	
	
	private String src;
	private String dist;
	
	
	private IdxType(String src, String dist) {
		this.src = src;
		this.dist = dist;
	}

	public String getDist() {
		return dist;
	}

	public void setDist(String dist) {
		this.dist = dist;
	}

	public String getSrc() {
		return src;
	}

	public void setSrc(String src) {
		this.src = src;
	}
	
}
