package edu.ut.debs2022;

import java.util.ArrayList;
import java.util.List;

import de.tum.i13.challenge.Batch;

public class BatchCacheSingleton {

	private static BatchCacheSingleton uniqueInstance;
	private List<Batch> cache;

	public static synchronized BatchCacheSingleton getInstance() {
		if (uniqueInstance == null) {
			uniqueInstance = new BatchCacheSingleton();
		}
		return uniqueInstance;
	}

	private BatchCacheSingleton() {
		
		// pre-allocate this batch
		//!TODO: read this preallocated size from some constants. 
		this.cache = new ArrayList<Batch>(10000);
	}

	public void addToQueue(Batch b) {
		this.cache.add(b);
	}

	public Batch getNext() {
		Batch b=null; 
		
		if(this.cache.size()!=0)
		  b = this.cache.remove(0);
	
		
		return b;
	}

	public int size() {

		return this.cache.size();
	}

}
