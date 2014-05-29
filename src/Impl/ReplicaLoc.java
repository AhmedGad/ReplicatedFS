package Impl;

public class ReplicaLoc {
	private String host;
	private String name;
	private int port;

	public ReplicaLoc(String host, String name, int port) {
		this.host = host;
		this.name = name;
		this.port = port;
	}
	
	public String getHost() {
		return host;
	}

	public String getName() {
		return name;
	}

	public int getPort() {
		return port;
	}
}
