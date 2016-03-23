package project2;


import java.util.Collections;
import java.util.Comparator;


// NodeName class contains IP address, port and ID of the nodes

public class NodeName implements Comparable<NodeName>{
	
	private String myIP;
	private int myPort;
	private int myID;

	public int compareTo(NodeName N) {
	        return Integer.compare(myID, N.myID);
	   }

	
	public void setID (int id) {
		this.myID = id;
	}
	public void setIP (String ip) {
		this.myIP = ip;
	}
	public void setPort (int port) {
		this.myPort = port;
	}
	public int getID() {
		return this.myID;
	}
	public String getIP() {
		return this.myIP;
	}
	public int getPort() {
		return this.myPort;
	}
	public NodeName(String ip, int port, int id){
		myID = id;
		myIP = ip;
		myPort = port;
	}
}
