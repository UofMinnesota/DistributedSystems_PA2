package project1;

import org.apache.thrift.TException;
import java.security.*;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.Random;
import java.util.Collections;
import java.util.Comparator;

import javax.print.DocFlavor.STRING;
import java.io.*;
import java.net.*;

import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class NodeServiceHandler implements NodeService.Iface {

  public static class  NodeInfo implements Comparable<NodeInfo>{
    String address = "";
    int port = 0;
    int hash = 0;


    public int compareTo(NodeInfo N) {
        return Integer.compare(hash, N.hash);
    }


  }

  private static ArrayList<NodeName> ListOfNodes = new ArrayList<NodeName>();
  private static Map<String, String> files = new HashMap<String, String>();

  private static String DHTList;
  private static int maxNumNodes = 16;
  private static NodeName myName;
  private static NodeName predecessor;
  private static int m;
  private static FingerTable[] fingerTable;
  private static int numDHT;
  private Random randomGenerator = new Random();

  public static ArrayList<NodeName> getListOfNodes(){

	  return ListOfNodes;
  }

  public NodeServiceHandler(int max)
  {
    if(max == -1) return;
    maxNumNodes = max;
  }

  public static NodeName getMyName(){
	  return myName;
  }

  public static int getNumberOfFiles()
  {
    return files.size();
  }

  public static ArrayList<String> getFileNames()
  {
    ArrayList<String> filesArr = new ArrayList<String>();
    for (String name: files.keySet()){

            String key =name.toString();
            String value = files.get(name).toString();
            //System.out.println(key);
            filesArr.add(key);



          }
    return filesArr;
  }

  int keyHash(String key)
  {
      int k = (int)key.length();
      int u = 0,n = 0;

      for (int i=0; i<k; i++)
      {
          n = (int)key.charAt(i);
          u += i*n%31;
      }
      return u%maxNumNodes;
  }

  public static void setConfig(String dht_list, String address, int port){
	  //System.out.println("1Entering setConfig of service handler...");
	  DHTList = dht_list;
	  ListOfNodes = strToNodeNameArray(DHTList);

	  myName = new NodeName(address, port, 0);
	  predecessor = new NodeName("NA", -1, -1);

	  //myName.setIP(address);
	  //myName.setPort(port);

	  //System.out.println("2Entering setConfig of service handler...");

	  myName.setID(findmyID(ListOfNodes));


	  //updating fingertable related values
	  m = (int) Math.ceil(Math.log(maxNumNodes) / Math.log(2));
	  //System.out.println("Value of m is "+ m);
      fingerTable = new FingerTable[m+1];
      numDHT = (int)Math.pow(2,m);

      findPredecessor();
      buildFingerTable();

      printFingerTable();



  }

  private static int findmyID(ArrayList<NodeName> nodeList){

	  int ID=-1;
	  //NodeName temp_node;

	  for(int i=0; i< nodeList.size(); i++){
		  //System.out.println("iterating over the list..."+ nodeList.get(i).getIP() + " " +nodeList.get(i).getPort() + " " + nodeList.get(i).getID() + " " + myName.getIP() + " " + myName.getPort() + " answer is " + nodeList.get(i).getIP().compareTo(myName.getIP()));
		  if((myName.getPort() == nodeList.get(i).getPort()) && nodeList.get(i).getIP().equals(myName.getIP())){
			  System.out.println("My ID found is "+ nodeList.get(i).getID());
			  ID = nodeList.get(i).getID();
			  break;
		  }
		  else{
			 // System.out.println("else iterating over the list..."+ nodeList.get(i).getIP() + " " +nodeList.get(i).getPort() + " " + nodeList.get(i).getID() + " " + myName.getIP() + " " + myName.getPort());
		  }

	  }

	  return ID;

  }

  private static void findPredecessor(){

	  //System.out.println("My ID is before sort in findPredecessor.."+myName.getID());
	  Collections.sort(ListOfNodes);
	  //System.out.println("My ID is after sort in findPredecessor.."+myName.getID());

	  /*for(int i=0;i<ListOfNodes.size();i++){
		  System.out.println("Array list sorted is " + ListOfNodes.get(i).getIP() + " " +ListOfNodes.get(i).getPort() + " " + ListOfNodes.get(i).getID() + " ");
	  }*/

	  //If there is only one node to the system the predecessor is the same node
	  if(ListOfNodes.size() == 1){
		  System.out.println("Only one node in the system...");
		  predecessor=myName;
	  }
	  //If there are multiple nodes, then predecessor is the previous element in the array
	  else{

		  int myLocation=0;

		  for(int i=0;i<ListOfNodes.size();i++){
			  if((myName.getPort() == ListOfNodes.get(i).getPort()) && ListOfNodes.get(i).getIP().equals(myName.getIP()) && (myName.getID() == ListOfNodes.get(i).getID())){
				  //System.out.println("Found the ID...");
				  myLocation = i;
				  break;
			  }
		  }

		  //System.out.println("My ID is.."+myName.getID());

		  if(myLocation == 0){
			  predecessor = new NodeName(ListOfNodes.get(ListOfNodes.size()-1).getIP(), ListOfNodes.get(ListOfNodes.size()-1).getPort(), ListOfNodes.get(ListOfNodes.size()-1).getID());
			  
		  }
		  else{
			  
			  System.out.println("Location is "+ myLocation);
			  predecessor = new NodeName(ListOfNodes.get(myLocation-1).getIP(), ListOfNodes.get(myLocation-1).getPort(), ListOfNodes.get(myLocation-1).getID());
			
		  }

	  }

	  //System.out.println("My ID is.."+myName.getID());

  }

  private static void buildFingerTable(){

	  //System.out.println("My ID is.."+myName.getID());

	  Collections.sort(ListOfNodes);

	  for(int i = 1; i <= m ; i++){
		  fingerTable[i] = new FingerTable();
		  fingerTable[i].setStart((myName.getID() + (int)Math.pow(2,i-1)) % numDHT);

		  //System.out.println("My Start Value is "+fingerTable[i].getStart()+" my ID is.."+ myName.getID());
	  }

	  int intEnd ;
	  for (int i = 1; i < m; i++) {
		  intEnd = (fingerTable[i+1].getStart()-1)%numDHT;
		  if(intEnd<0){intEnd=intEnd+numDHT;}
          fingerTable[i].setInterval(fingerTable[i].getStart(),intEnd);
      }
	  intEnd = (fingerTable[1].getStart()-1)%numDHT;
	  if(intEnd<0){intEnd=intEnd+numDHT;}
      fingerTable[m].setInterval(fingerTable[m].getStart(),intEnd);

      if (predecessor.getID() == myName.getID()) { //if predecessor is same as my ID -> only node in DHT
          for (int i = 1; i <= m; i++) {
              fingerTable[i].setSuccessor(myName);
          }
          System.out.println("Done, all finger tablet set as myName (only node in DHT)");
      }
      else {
          for (int i = 1; i <= m; i++) {
              fingerTable[i].findSuccessor(myName, ListOfNodes, numDHT);
          }
      }

      //System.out.println("My ID is.."+myName.getID());
  }

  public static void printFingerTable(){

	  System.out.println("Node ID is "+myName.getID());
	  System.out.println("Range of Keys: " + (predecessor.getID()+1)%numDHT + " - " + myName.getID());
	  System.out.println("Predecessor Node: "+predecessor.getIP()+":"+predecessor.getPort()+":"+predecessor.getID());
	  System.out.println("Successor Node: "+fingerTable[1].getSuccessor().getIP()+":"+fingerTable[1].getSuccessor().getPort()+":"+fingerTable[1].getSuccessor().getID());
	  System.out.println("Number of Files Stored:"+getNumberOfFiles());
	  System.out.println("List of Files Stored:");
	  ArrayList<String> fileList = getFileNames();
	  Collections.sort(fileList);
	  for(int i =0; i<fileList.size();i++){
		  System.out.println(fileList.get(i));
	  }
	  System.out.println("Finger Table after the update:");
	  System.out.println("  |  "+"Start"+"  |  "+"Interval Begin"+"  |  "+"Interval End"+"  |  "+"Successor"+"  |  ");
	  for(int i = 1; i <= m ; i++){

		System.out.println("      |      "+fingerTable[i].getStart()+"      |      "+fingerTable[i].getIntervalBegin()+"      |      "+fingerTable[i].getIntervalEnd()+"      |      "+fingerTable[i].getSuccessor().getID()+"      |      ");
	  }

  }


  public static NodeName strToNodeName(String input)
  {
    String data[] = input.split(":");
    NodeName newNo = new NodeName(data[0].trim(),Integer.parseInt(data[1]),Integer.parseInt(data[2]));
  
    return newNo;
  }

  public static ArrayList<NodeName> strToNodeNameArray(String input)
  {
    ArrayList<NodeName> arrN = new ArrayList<NodeName>();
    String data[] = input.split("\\,");
    for(int c = 0; c < data.length; c++)
    {
      arrN.add(strToNodeName(data[c]));
    }
    return arrN;
  }

  public String MD5(String md5) {
     try {
          java.security.MessageDigest md = java.security.MessageDigest.getInstance("MD5");
          byte[] array = md.digest(md5.getBytes());
          StringBuffer sb = new StringBuffer();
          for (int i = 0; i < array.length; ++i) {
            sb.append(Integer.toHexString((array[i] & 0xFF) | 0x100).substring(1,3));
         }
          return sb.toString();
      } catch (java.security.NoSuchAlgorithmException e) {

      return null;}
  }

public int isSuccessor(int hash)
{
  return -1;
}



 @Override
 public boolean Write(String Filename, String Contents) throws TException {
  //String NodeList = " ";

  int hash = keyHash(Filename);
  boolean writeComplete=false;

  System.out.println("Request for writing Filename "+Filename+"to this node...");

  System.out.println("My ID is.."+myName.getID()+" and my Predecessor in the DHT is.."+predecessor.getID()+" and the key of this file is.."+hash);

  if(myName.getID() < predecessor.getID()){ //my ID can be 2 and predecessor ID can be 6
	  if(hash > predecessor.getID() || hash <= myName.getID()){
		  System.out.println("This file "+ Filename+ " with ID "+hash+"belongs to me..");
		  files.put(Filename, Contents);
		  System.out.println("File: "+ Filename+ " has been added to this node. Node Details after file addition:");
		  printFingerTable();
		  writeComplete = true;
		  return writeComplete;
	  }
  }

  if(myName.getID() >= predecessor.getID()){ //my ID can be 3 predecessor can be 0
	  if(hash > predecessor.getID() && hash <= myName.getID()){
		  //System.out.println("This file "+ Filename+ " with ID "+hash+"belongs to me..");
		  files.put(Filename, Contents);
		  System.out.println("File: "+ Filename+ " has been added to this node. Node Details after file addition:");
		  printFingerTable();
		  writeComplete = true;
		  return writeComplete;
	  }
  }



  //int succ = isSuccessor(hash);
  //if(succ == -1)
  //{
  //  files.put(Filename, Contents);

  //  return true;
  //}

  //If the key is not in-between predecessor and me forward the write to fingertable
  System.out.println("File does not belong to me..Forwarding to a successor...");

  if(writeComplete == false)
  {

	  for(int i=m;i>=1;i--){
		  System.out.println("Iterating to ID "+fingerTable[i].getSuccessor().getID());

		  if(((fingerTable[i].getIntervalBegin()>fingerTable[i].getIntervalEnd())&& (hash>=fingerTable[i].getIntervalBegin() || hash<=fingerTable[i].getIntervalEnd())) ||
				  ((fingerTable[i].getIntervalBegin()<=fingerTable[i].getIntervalEnd())&& (hash>=fingerTable[i].getIntervalBegin() && hash<=fingerTable[i].getIntervalEnd()))){
			  	TTransport NodeTransport;
				System.out.println("Request forwarded to.. " +fingerTable[i].getSuccessor().getIP()+" "+fingerTable[i].getSuccessor().getPort()+ " "+fingerTable[i].getSuccessor().getID() );
			    NodeTransport = new TSocket(fingerTable[i].getSuccessor().getIP(), fingerTable[i].getSuccessor().getPort()); //map nodes in the ports
			    NodeTransport.open();

			    TProtocol NodeProtocol = new TBinaryProtocol(NodeTransport);

			    NodeService.Client nodeclient = new NodeService.Client(NodeProtocol);

			    boolean returnVal = nodeclient.Write(Filename, Contents);

			    NodeTransport.close();

			    return returnVal;
		  }

	  }

	  if(hash>fingerTable[m].getSuccessor().getID()){
		  	TTransport NodeTransport;
			System.out.println("Request forwarded to.. " +fingerTable[m].getSuccessor().getIP()+" "+fingerTable[m].getSuccessor().getPort()+ " "+fingerTable[m].getSuccessor().getID() );
		    NodeTransport = new TSocket(fingerTable[m].getSuccessor().getIP(), fingerTable[m].getSuccessor().getPort()); //map nodes in the ports
		    NodeTransport.open();

		    TProtocol NodeProtocol = new TBinaryProtocol(NodeTransport);

		    NodeService.Client nodeclient = new NodeService.Client(NodeProtocol);

		    boolean returnVal = nodeclient.Write(Filename, Contents);

		    NodeTransport.close();

		    return returnVal;
	  }
  }

  return false;
 }

 @Override
 public String Read(String Filename) throws TException {
   //String NodeList = " ";

   int hash = keyHash(Filename);
   //System.out.println("Filename is "+Filename);
   boolean readComplete=false;

   System.out.println("Request for reading Filename "+Filename+" came to this node...");

   System.out.println("My ID is.. "+myName.getID()+" and my Predecessor in the DHT is.. "+predecessor.getID()+" and the key of this file is.."+hash);

   if(myName.getID() < predecessor.getID()){ //my ID can be 2 and predecessor ID can be 6
 	  if(hash > predecessor.getID() || hash <= myName.getID()){
 		  System.out.println("This file "+ Filename+ " with ID "+hash+"belongs to me..");
 		  
 		  readComplete = true;
 		
 		 System.out.println("File: "+ Filename+ " was tried to read this node. Node Details after file addition: ");
		  printFingerTable();
      if(files.containsKey(Filename)) return (files.get(Filename));
      return "*** FILE NOT FOUND ***";
 	  }
   }

   if(myName.getID() >= predecessor.getID()){ //my ID can be 3 predecessor can be 0
 	  if(hash > predecessor.getID() && hash <= myName.getID()){
 		  System.out.println("This file "+ Filename+ " with ID "+hash+"belongs to me..");
 		  
 		  readComplete = true;
 		  
 		 System.out.println("File: "+ Filename+ " was tried to read this node. Node Details after file addition: ");
		  printFingerTable();
      if(files.containsKey(Filename)) return (files.get(Filename));
      return "*** FILE NOT FOUND ***";
 	  }
   }


 //If the key is not in-between predecessor and me forward the write to fingertable
   System.out.println("File does not belong to me..Forwarding to a successor...");

   if(readComplete == false)
   {

		  for(int i=m;i>=1;i--){
			  System.out.println("Iterating to ID "+fingerTable[i].getSuccessor().getID());

			  if(((fingerTable[i].getIntervalBegin()>fingerTable[i].getIntervalEnd())&& (hash>=fingerTable[i].getIntervalBegin() || hash<=fingerTable[i].getIntervalEnd())) ||
					  ((fingerTable[i].getIntervalBegin()<=fingerTable[i].getIntervalEnd())&& (hash>=fingerTable[i].getIntervalBegin() && hash<=fingerTable[i].getIntervalEnd()))){
				  	TTransport NodeTransport;
					System.out.println("Request forwarded to.. " +fingerTable[i].getSuccessor().getIP()+" "+fingerTable[i].getSuccessor().getPort()+ " "+fingerTable[i].getSuccessor().getID() );
				    NodeTransport = new TSocket(fingerTable[i].getSuccessor().getIP(), fingerTable[i].getSuccessor().getPort()); //map nodes in the ports
				    NodeTransport.open();

				    TProtocol NodeProtocol = new TBinaryProtocol(NodeTransport);

		 		    NodeService.Client nodeclient = new NodeService.Client(NodeProtocol);


		 		   String readVal = nodeclient.Read(Filename);

		 		    NodeTransport.close();

		 		    return readVal;

			  }

		  }

		  if(hash>fingerTable[m].getSuccessor().getID()){
			  	TTransport NodeTransport;
				System.out.println("Request forwarded to.. " +fingerTable[m].getSuccessor().getIP()+" "+fingerTable[m].getSuccessor().getPort()+ " "+fingerTable[m].getSuccessor().getID() );
			    NodeTransport = new TSocket(fingerTable[m].getSuccessor().getIP(), fingerTable[m].getSuccessor().getPort()); //map nodes in the ports
			    NodeTransport.open();

			    TProtocol NodeProtocol = new TBinaryProtocol(NodeTransport);

	 		    NodeService.Client nodeclient = new NodeService.Client(NodeProtocol);


	 		   String readVal = nodeclient.Read(Filename);

	 		    NodeTransport.close();

	 		    return readVal;
		  }

   }
   return "File not Found..";
 }

 @Override
 public boolean UpdateDHT(String NodeList) throws TException {

	 System.out.println("A new node has joined the network. Updating the finger table...");
	 System.out.println("My ID is.."+myName.getID());
	 DHTList=NodeList;
	 ListOfNodes = strToNodeNameArray(NodeList);
     findPredecessor();
     buildFingerTable();
     printFingerTable();



  return true;
 }


}
