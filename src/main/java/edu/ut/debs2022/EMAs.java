package edu.ut.debs2022;

public class EMAs {

	private float ema38;
	private float ema100;

	public EMAs() {
	}

	public EMAs(float ema38, float ema100) {

		this.ema100 = ema100;
		this.ema38 = ema38;

	}

	public float getEma38() {
		return ema38;
	}

	public void setEma38(float ema38) {
		this.ema38 = ema38;
	}

	public float getEma100() {
		return ema100;
	}

	public void setEma100(float ema100) {
		this.ema100 = ema100;
	}

}
