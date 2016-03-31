package project2;

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

public class FileServiceHandler implements FileService.Iface {

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
  //private static int maxNumNodes = 16;
  private static boolean isCoordinator = false;
  private static NodeName CoordinatorName;
  private static NodeName myName;
  private static String joinResult;
  private static int Nr, Nw, N;
  private Random randomGenerator = new Random();

  public static ArrayList<NodeName> getListOfNodes(){

	  return ListOfNodes;
  }

  public FileServiceHandler(boolean isC, String name, int port, String result)
  {
    //if(max == -1) return;
    //maxNumNodes = max;
	  myName = new NodeName(name,port,0);
	  isCoordinator = isC;
	  joinResult = result;
	  //myName.setIP(name);
	  //myName.setPort(port);
	  System.out.println("Result obtained is "+ joinResult);
  }

  public static NodeName getMyName(){
	  return myName;
  }

  public static ArrayList<NodeName> getNodes()
  {
    return ListOfNodes;
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




  public static NodeName strToNodeName(String input)
  {
    String data[] = input.split(":");
    NodeName newNo = new NodeName(data[0].trim(),Integer.parseInt(data[1]),0);

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



 @Override
 public boolean clientWrite(String Filename, String Contents) throws TException {

	 boolean result= false;

	 // TODO send request to coordinator if I am not the coordinator
	 if(isCoordinator){
		 System.out.println("\n\n\nI am the coordinator... and I got a write request\n\n\n\n");

	 }
	 else{
		 System.out.println("\n\n\nI am NOT the coordinator... and I got a write request\n\n\n\n");
			try {
				TTransport NodeTransport;
				System.out.println("Connecting to: " + CoordinatorName.getIP() + ":" + CoordinatorName.getPort());
				NodeTransport = new TSocket(CoordinatorName.getIP(),CoordinatorName.getPort());
				NodeTransport.open();

				TProtocol NodeProtocol = new TBinaryProtocol(NodeTransport);
				FileService.Client nodeclient = new FileService.Client(
						NodeProtocol);

				result = nodeclient.serverWriteReq(Filename, Contents);


				NodeTransport.close();
			} catch (TException xx) {
				xx.printStackTrace();
			}

	 }

   System.out.println("Request received for writing file"+Filename+" with contents "+ Contents );
  files.put(Filename, Contents);
  return result;
 }


 @Override
 public String clientRead(String Filename) throws TException {
     if(files.containsKey(Filename)) return (files.get(Filename));
     return "*** FILE NOT FOUND ***";
 }



 @Override
 public boolean serverWrite(String Filename, String Contents) throws TException {
	 System.out.println("Request received for writing file"+Filename+" with contents "+ Contents );
	  files.put(Filename, Contents);
	  return true;
 }


 @Override
 public String serverRead(String Filename) throws TException {

     if(files.containsKey(Filename)) return (files.get(Filename));
     return "*** FILE NOT FOUND ***";
 }


 @Override
 public boolean serverWriteReq(String Filename, String Contents) throws TException {
	 System.out.println("Request for write of file "+ Filename+" and contents "+Contents+" Came to Coordinator...\nAssembling write quorom...");

	 //Assembling write quorom here
	 Nw=randInt(Math.round((N+1)/2),N);
	 System.out.println("Write quorom size is.."+Nw);

	 ArrayList<Integer> quorom_indexes = uniquerands(Nw,N);

	 for(int i=0; i<quorom_indexes.size();i++){
		 System.out.println("Write list is .."+quorom_indexes.get(i));
	 }


	 System.out.println("Request received for writing file"+Filename+" with contents "+ Contents );
	  files.put(Filename, Contents);
	  return true;
 }


 @Override
 public String serverReadReq(String Filename) throws TException {

     if(files.containsKey(Filename)) return (files.get(Filename));
     return "*** FILE NOT FOUND ***";
 }

 @Override
 public int getVersionNumber(String Filename) throws TException {
   return 0;
 }

 @Override
 public boolean makeCoordinator(String ServerList) throws TException {

	 System.out.println("List of servers obtained..."+ServerList);
	 ListOfNodes=strToNodeNameArray(ServerList);
	 N = ListOfNodes.size();
	 System.out.println("Total size of replica Network ..."+N);
   return true;
 }

 @Override
 public boolean setCoordinatorInfo(String Coordinator) throws TException{

	 CoordinatorName = strToNodeName(Coordinator);
	return true;
 }

 public static int randInt(int min, int max) {


 Random rand = new Random();
 int randomNum = rand.nextInt((max - min) + 1) + min;

 return randomNum;
}

 public static ArrayList<Integer> uniquerands(int required, int total){
	 ArrayList<Integer> list = new ArrayList<Integer>();
	 ArrayList<Integer> output = new ArrayList<Integer>();
     for (int i=0; i<total; i++) {
         list.add(new Integer(i));
     }
     Collections.shuffle(list);
     for (int i=0; i<required; i++) {
         output.add(list.get(i));
     }

     return output;

 }



}
