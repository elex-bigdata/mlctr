package com.elex.ssp.mlctr.vector;

public enum FeaturePrefix {

	browser("browser","ua","bw"),
	area("area","ip","ip"),
	user("user","u","u"),
	project("project","p","p"),
	odp("odp","odp","odp"),
	gdp("gdp","gdp","gk"),
	ssp("ssp","ssp","kw"),
	os("os","os","os"),
	nation("nation","na","na"),
	adid("adid","ad","ad"),
	ref("ref","ref","rf"),
	opt("opt","opt","opt"),
	time("time","t","t");
	
	private String fullName;
	private String thowName;
	private String sName;
	
	private FeaturePrefix(String fullName, String sName,String thowName) {
		this.fullName = fullName;
		this.sName = sName;
		this.thowName = thowName;
	}
	
	public String getFullName() {
		return fullName;
	}

	public void setFullName(String fullName) {
		this.fullName = fullName;
	}

	public String getThowName() {
		return thowName;
	}

	public void setThowName(String thowName) {
		this.thowName = thowName;
	}

	public String getsName() {
		return sName;
	}

	public void setsName(String sName) {
		this.sName = sName;
	}
	
}
