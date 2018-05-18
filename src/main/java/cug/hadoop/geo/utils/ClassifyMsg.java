package cug.hadoop.geo.utils;

public class ClassifyMsg {

	private  float min;
	private float max;
	private float level;
	
	public ClassifyMsg(float min , float max , float level){
		       this.min = min;
		       this.max = max;
		       this.level = level;
	}

	public float getMin() {
		return min;
	}

	public void setMin(float min) {
		this.min = min;
	}

	public float getMax() {
		return max;
	}

	public void setMax(float max) {
		this.max = max;
	}

	public float getLevel() {
		return level;
	}

	public void setLevel(float level) {
		this.level = level;
	}
}
