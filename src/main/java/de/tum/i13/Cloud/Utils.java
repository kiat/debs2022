package de.tum.i13.Cloud;

public class Utils {
	
	public static class Const {
		public static int N_THREADS = 3;
	}

	public static int hashIt(String key, int max) {
		int hash = 7;
		for (int i = 0; i < key.length(); i++) {
			hash = (hash * 7 + key.charAt(i)) % max;
		}
		return hash;
	}

	public static int getPort(String host) {
		return Integer.parseInt(host.split(":")[1]);
	}

	public static String getAddr(String host) {
		return host.split(":")[0];
	}
}
