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
  private static int maxNumNodes = 16;
  private static NodeName myName;
  private Random randomGenerator = new Random();

  public static ArrayList<NodeName> getListOfNodes(){

	  return ListOfNodes;
  }

  public FileServiceHandler(int max)
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



 @Override
 public boolean clientWrite(String Filename, String Contents) throws TException {
  return false;
 }

 @Override
 public boolean serverWrite(String Filename, String Contents) throws TException {
  return false;
 }
 
 @Override
 public String clientRead(String Filename) throws TException {
   return "File not Found..";
 }

 @Override
 public String serverRead(String Filename) throws TException {
   return "File not Found..";
 }

 @Override
 public int getVersionNumber(String Filename) throws TException {
   return 0;
 }

 @Override
 public boolean makeCoordinator(String ServerList) throws TException {
   return false;
 }

}
