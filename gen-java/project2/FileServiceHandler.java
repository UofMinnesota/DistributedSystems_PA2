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
import java.util.PriorityQueue;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
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

  public static class  UpdateInfo{
    String address = "";
    int port = 0;
    int timestamp = 0;
    Boolean isWrite = false;
    String filename = "";
    String content = "";


  }

  private static ArrayList<NodeName> ListOfNodes = new ArrayList<NodeName>();
  private static Map<String, FileStore> filestore = new HashMap<String, FileStore>();

  private static String DHTList;
  //private static int maxNumNodes = 16;
  private static boolean isCoordinator = false;
  private static boolean isRunningBg = false;
  private static NodeName CoordinatorName;
  private static NodeName myName;
  private static String joinResult;
  private static int Nr, Nw, N;
  public static BlockingQueue executeQueue = new ArrayBlockingQueue(1024);
  private Random randomGenerator = new Random();

  public static ArrayList<NodeName> getListOfNodes(){

	  return ListOfNodes;
  }

  public void setJoinResult(String T){
	  joinResult = T;

	  if(isCoordinator){
		  ListOfNodes=strToNodeNameArray(joinResult);
		  N = ListOfNodes.size();
	  }
	  else{
		  CoordinatorName = strToNodeName(joinResult);
	  }
  }


  public FileServiceHandler(boolean isC, String name, int port)
  {
    //if(max == -1) return;
    //maxNumNodes = max;
	  myName = new NodeName(name,port,0);
	  isCoordinator = isC;

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
    return filestore.size();
  }

  public static ArrayList<FileStore> getFileNames()
  {
    ArrayList<FileStore> filestoreArr = new ArrayList<FileStore>();
    for (String name: filestore.keySet()){

            String key =name.toString();
            FileStore value = filestore.get(name);
            //System.out.println(key);
            filestoreArr.add(value);



          }
    return filestoreArr;
  }



  public static NodeName strToNodeName(String input)
  {
    System.out.println("Input is "+input);
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
		 result = serverWriteReq(Filename, Contents);

	 }
	 else{
		 System.out.println("\n\n\nI am NOT the coordinator... and I got a write request\n\n\n\n");
			try {
				System.out.println("Before transport");
				TTransport NodeTransport;
				System.out.println("After transport");
				System.out.println("Connecting to: " + CoordinatorName.getIP() + ":" + CoordinatorName.getPort());
				NodeTransport = new TSocket(CoordinatorName.getIP(),CoordinatorName.getPort());
				NodeTransport.open();

				TProtocol NodeProtocol = new TBinaryProtocol(NodeTransport);
				FileService.Client nodeclient = new FileService.Client(
						NodeProtocol);
				System.out.println("Forwarding the request for writing file"+Filename+" with contents "+ Contents+" to the coordinator..." );
				result = nodeclient.serverWriteReq(Filename, Contents);


				NodeTransport.close();
			} catch (TException xx) {
				xx.printStackTrace();
			}

	 }

  //filestore.put(Filename, Contents);
  return result;
 }


 @Override
 public String clientRead(String Filename) throws TException {
	 String result= "*** FILE NOT FOUND ***";

	 // TODO send request to coordinator if I am not the coordinator
	 if(isCoordinator){
		 System.out.println("\n\n\nI am the coordinator... and I got a write request\n\n\n\n");
		 result = serverReadReq(Filename);

	 }
	 else{
		 System.out.println("\n\n\nI am NOT the coordinator... and I got a write request\n\n\n\n");
			try {
				System.out.println("Before transport in Read..");
				TTransport NodeTransport;
				System.out.println("After transport in Read..");
				System.out.println("Connecting to: " + CoordinatorName.getIP() + ":" + CoordinatorName.getPort());
				NodeTransport = new TSocket(CoordinatorName.getIP(),CoordinatorName.getPort());
				NodeTransport.open();

				TProtocol NodeProtocol = new TBinaryProtocol(NodeTransport);
				FileService.Client nodeclient = new FileService.Client(
						NodeProtocol);
				System.out.println("Forwarding the request for reading file "+Filename+"  to the coordinator..." );
				result = nodeclient.serverReadReq(Filename);

				NodeTransport.close();
			} catch (TException xx) {
				xx.printStackTrace();
			}

	 }

  //filestore.put(Filename, Contents);
     return result;
 }



 @Override
 public boolean serverWrite(String Filename, String Contents, int Version) throws TException {
	 System.out.println("Request received for writing file"+Filename+" with contents "+ Contents );

	 FileStore file = new FileStore(Filename,Contents, Version);

	  filestore.put(Filename, file);
	  return true;
 }


 @Override
 public String serverRead(String Filename) throws TException {

     if(filestore.containsKey(Filename)) return (filestore.get(Filename).getContents());
     return "*** FILE NOT FOUND ***";
 }


public void asyncServerWriteReq(UpdateInfo updIn) throws TException
{
  String Filename = updIn.filename;
  String Contents = updIn.content;
  boolean result=false;
  System.out.println("Request for write of file "+ Filename+" and contents "+Contents+" Came to Coordinator...\nAssembling write quorom...");

  //Assembling write quorom here
  Nw=randInt(Math.round((N+1)/2),N);
  System.out.println("Write quorom size is.."+Nw);

  ArrayList<Integer> quorom_indexes = uniquerands(Nw,N);
  int latestVersion=0;
  int version=0;

  for(int i=0; i<quorom_indexes.size();i++){
    System.out.println("Write list is .."+quorom_indexes.get(i));
    System.out.println("Node Names are "+ListOfNodes.get(quorom_indexes.get(i)).getIP());
   if(myName.getIP().equals(ListOfNodes.get(quorom_indexes.get(i)).getIP()) && myName.getPort() == ListOfNodes.get(quorom_indexes.get(i)).getPort()){
     System.out.println("Connecting to: " + ListOfNodes.get(quorom_indexes.get(i)).getIP() + ":" + ListOfNodes.get(quorom_indexes.get(i)).getPort());

     System.out.println("Enquiring latest version for the file "+Filename+" to the Quorom..." );
     version = getVersionNumber(Filename);
     System.out.println("Version for the file "+Filename+" from "+ ListOfNodes.get(quorom_indexes.get(i)).getIP()+":"+ListOfNodes.get(quorom_indexes.get(i)).getPort() +" of the Quorom... is "+version );

     if(version>latestVersion){
       latestVersion = version;
     }

   }
   else{
    try {

       TTransport NodeTransport;
       System.out.println("Connecting to: " + ListOfNodes.get(quorom_indexes.get(i)).getIP() + ":" + ListOfNodes.get(quorom_indexes.get(i)).getPort());
       NodeTransport = new TSocket(ListOfNodes.get(quorom_indexes.get(i)).getIP(),ListOfNodes.get(quorom_indexes.get(i)).getPort());
       NodeTransport.open();

       TProtocol NodeProtocol = new TBinaryProtocol(NodeTransport);
       FileService.Client nodeclient = new FileService.Client(
           NodeProtocol);
       System.out.println("Enquiring latest version for the file "+Filename+" to the Quorom..." );
       version = nodeclient.getVersionNumber(Filename);
       System.out.println("Version for the file "+Filename+" from "+ ListOfNodes.get(quorom_indexes.get(i)).getIP()+":"+ListOfNodes.get(quorom_indexes.get(i)).getPort() +" of the Quorom... is "+version );

       if(version>latestVersion){
         latestVersion = version;
       }

       NodeTransport.close();
     } catch (TException xx) {
       xx.printStackTrace();
     }
    }
  }

  System.out.println("Latest Version for the file "+Filename+" is "+latestVersion );
  System.out.println("Writing back file "+Filename+" as version "+latestVersion++ );


  for(int i=0; i<quorom_indexes.size();i++){
    System.out.println("Write list is .."+quorom_indexes.get(i));
    System.out.println("Node Names are "+ListOfNodes.get(quorom_indexes.get(i)).getIP());
    if(myName.getIP().equals(ListOfNodes.get(quorom_indexes.get(i)).getIP()) && myName.getPort() == ListOfNodes.get(quorom_indexes.get(i)).getPort()){

       System.out.println("Writing File "+Filename+" with latest version "+latestVersion+" to the Quorom..." );
       result = serverWrite(Filename, Contents, latestVersion);

    }
    else{
      try {

       TTransport NodeTransport;
       NodeTransport = new TSocket(ListOfNodes.get(quorom_indexes.get(i)).getIP(),ListOfNodes.get(quorom_indexes.get(i)).getPort());
       NodeTransport.open();

       TProtocol NodeProtocol = new TBinaryProtocol(NodeTransport);
       FileService.Client nodeclient = new FileService.Client(
           NodeProtocol);
       System.out.println("Writing File "+Filename+" with latest version "+latestVersion+" to the Quorom..." );
       result = nodeclient.serverWrite(Filename, Contents, latestVersion);

       NodeTransport.close();
     } catch (TException xx) {
       xx.printStackTrace();
     }
    }
  }


  System.out.println("Request received for writing file"+Filename+" with contents "+ Contents );
   //files.put(Filename, Contents);
   //return result;
}

 @Override
 public boolean serverWriteReq(String Filename, String Contents) throws TException {
   try {
      if(isRunningBg == false)
      {
        try{
          Runnable simple = new Runnable() {
            public void run() {
              //updateReplicas();

              try{
              //executeQueue.put(newInfo);
              asyncServerWriteReq((UpdateInfo)executeQueue.take());
              Thread.sleep(100);
            }
            catch(Exception e){
              System.out.println("Not added..");
            }
              //
            }
          };

          new Thread(simple).start();
          System.out.println("Initializing BG queue...");
          isRunningBg = true;
        } catch (Exception x) {
          x.printStackTrace();
        }
        }


       Runnable simple = new Runnable() {
         public void run() {
           //updateReplicas();
           long unixTime = System.currentTimeMillis() / 1000L;
           UpdateInfo newInfo = new UpdateInfo();
           newInfo.filename = Filename;
           newInfo.content = Contents;
           try{
           executeQueue.put(newInfo);
         }
         catch(Exception e){
           System.out.println("Not added..");
         }
           //asyncServerWriteReq(Filename, Contents, unixTime);
         }
       };

       new Thread(simple).start();
       System.out.println("Issuing serverWriteReq...");
     } catch (Exception x) {
       x.printStackTrace();
     }
     return true;
 }


 @Override
 public String serverReadReq(String Filename) throws TException {


	 String result="*** FILE NOT FOUND ***";
	 System.out.println("Request for Reading file "+ Filename+" came to Coordinator...\nAssembling write quorom...");

	 //Assembling read quorom here
	 Nw=randInt(N-Nw,N);
	 System.out.println("Total Number of Replicas "+N+"Write quorom size is.."+Nw+" Read Quorum size is "+Nr);

	 ArrayList<Integer> quorom_indexes = uniquerands(Nw,N);
	 int latestVersion=0;
	 int version=0;

	 for(int i=0; i<quorom_indexes.size();i++){
		 System.out.println("Read list is .."+quorom_indexes.get(i));
		 System.out.println("Node Names are "+ListOfNodes.get(quorom_indexes.get(i)).getIP());
	  if(myName.getIP().equals(ListOfNodes.get(quorom_indexes.get(i)).getIP()) && myName.getPort() == ListOfNodes.get(quorom_indexes.get(i)).getPort()){
			System.out.println("Connecting to: " + ListOfNodes.get(quorom_indexes.get(i)).getIP() + ":" + ListOfNodes.get(quorom_indexes.get(i)).getPort());

			System.out.println("Enquiring latest version for the file "+Filename+" to the Quorom..." );
			version = getVersionNumber(Filename);
			System.out.println("Version for the file "+Filename+" from "+ ListOfNodes.get(quorom_indexes.get(i)).getIP()+":"+ListOfNodes.get(quorom_indexes.get(i)).getPort() +" of the Quorom... is "+version );

			if(version>latestVersion){
				latestVersion = version;
			}

	  }
	  else{
		 try {

				TTransport NodeTransport;
				System.out.println("Connecting to: " + ListOfNodes.get(quorom_indexes.get(i)).getIP() + ":" + ListOfNodes.get(quorom_indexes.get(i)).getPort());
				NodeTransport = new TSocket(ListOfNodes.get(quorom_indexes.get(i)).getIP(),ListOfNodes.get(quorom_indexes.get(i)).getPort());
				NodeTransport.open();

				TProtocol NodeProtocol = new TBinaryProtocol(NodeTransport);
				FileService.Client nodeclient = new FileService.Client(
						NodeProtocol);
				System.out.println("Enquiring latest version for the file "+Filename+" to the Quorom..." );
				version = nodeclient.getVersionNumber(Filename);
				System.out.println("Version for the file "+Filename+" from "+ ListOfNodes.get(quorom_indexes.get(i)).getIP()+":"+ListOfNodes.get(quorom_indexes.get(i)).getPort() +" of the Quorom... is "+version );

				if(version>latestVersion){
					latestVersion = version;
				}

				NodeTransport.close();
			} catch (TException xx) {
				xx.printStackTrace();
			}
		 }
	 }

	 System.out.println("Latest Version for the file "+Filename+" is "+latestVersion );
	 System.out.println("Reading back file "+Filename+" as version "+latestVersion );


	 for(int i=0; i<quorom_indexes.size();i++){
		 System.out.println("Read list is .."+quorom_indexes.get(i));
		 System.out.println("Node Names are "+ListOfNodes.get(quorom_indexes.get(i)).getIP());
		 if(myName.getIP().equals(ListOfNodes.get(quorom_indexes.get(i)).getIP()) && myName.getPort() == ListOfNodes.get(quorom_indexes.get(i)).getPort()){

				System.out.println("Reading File "+Filename+" with latest version "+latestVersion+" to the Quorom..." );
				result = serverRead(Filename);

		 }
		 else{
			 try {

				TTransport NodeTransport;
				NodeTransport = new TSocket(ListOfNodes.get(quorom_indexes.get(i)).getIP(),ListOfNodes.get(quorom_indexes.get(i)).getPort());
				NodeTransport.open();

				TProtocol NodeProtocol = new TBinaryProtocol(NodeTransport);
				FileService.Client nodeclient = new FileService.Client(
						NodeProtocol);
				System.out.println("Reading File "+Filename+" with latest version "+latestVersion+" to the Quorom..." );
				result = nodeclient.serverRead(Filename);

				NodeTransport.close();
			} catch (TException xx) {
				xx.printStackTrace();
			}
		 }
	 }


	 System.out.println("Request completed for file"+Filename+" with contents "+ result );
	  //files.put(Filename, Contents);
	  return result;

 }

 @Override
 public int getVersionNumber(String Filename) throws TException {
	 if(filestore.containsKey(Filename)) return (filestore.get(Filename).getVersion());
	 return -1;
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
