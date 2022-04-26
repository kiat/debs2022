package edu.ut.debs2022;

import java.util.List;

import de.tum.i13.challenge.CrossoverEvent;
import de.tum.i13.challenge.Indicator;

public class TupleListResult {
	
	// This class holds the results for Query 1 and Query 2. 

	List<Indicator> indicatorList;
	List<CrossoverEvent> crossoverEventList;


	public TupleListResult(List<Indicator> indicatorList, List<CrossoverEvent> crossoverEventList) {

		this.crossoverEventList = crossoverEventList;
		this.indicatorList = indicatorList;
	}

	public List<CrossoverEvent> getCrossoverEventList() {
		return crossoverEventList;
	}

	public void setCrossoverEventList(List<CrossoverEvent> crossoverEventList) {
		this.crossoverEventList = crossoverEventList;
	}

	public List<Indicator> getIndicatorList() {
		return indicatorList;
	}

	public void setIndicatorList(List<Indicator> indicatorList) {
		this.indicatorList = indicatorList;
	}

}
