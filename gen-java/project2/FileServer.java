package project2;

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
import java.util.Random;
import org.apache.thrift.TException;
import java.util.ArrayList;
import java.util.Random;


//Node Class

public class FileServer {
  public static int randInt(int min, int max) {


  Random rand = new Random();
  int randomNum = rand.nextInt((max - min) + 1) + min;

  return randomNum;
}
  
  public FileServer(){
	  getHostAddress();
	  
  }
static boolean USE_LOCAL = false;
static boolean isCoordinator = false;
static int nodePort;
static String nodeName;
 public static void StartsimpleServer(FileService.Processor<FileServiceHandler> processor) {
  try {
	  //nodeName = new String(getHostAddress());
     
   TServerTransport serverTransport = new TServerSocket(nodePort);
   TServer server = new TSimpleServer(
     new Args(serverTransport).processor(processor));

   System.out.println("Establishing connection with the SuperNode...");

   TTransport SuperNodeTransport;
   String supernodeAddr = "csel-x30-10";
   if(USE_LOCAL) supernodeAddr = "localhost";
   SuperNodeTransport = new TSocket(supernodeAddr, 9090); // csel-x29-10
   SuperNodeTransport.open();

   TProtocol SuperNodeProtocol = new TBinaryProtocol(SuperNodeTransport);
   SuperNodeService.Client supernodeclient = new SuperNodeService.Client(SuperNodeProtocol);

  System.out.println("Requesting SuperNode for joining Replica Network through Join Call...");
  
  //nodeName = getHostAddress();
  System.out.println("My name is"+nodeName+"my port is "+nodePort);
  if(supernodeclient.Join(getHostAddress(),nodePort, isCoordinator)){
  System.out.println("The returned list is "+ supernodeclient.GetNodeList());
  }

  else{
	   System.out.println("Supernode busy...");
      }
	   
    System.out.println("Joining Replica network...");

	   ArrayList<NodeName> ListOfNodes = new ArrayList<NodeName>();
	   NodeName myName;


       //send DHTList string to the nodeservicehandler
       myName = FileServiceHandler.getMyName();
       ListOfNodes = FileServiceHandler.getListOfNodes();


	   SuperNodeTransport.close();

	   System.out.println("Successfully joined replica network...");
	   System.out.println("Starting simple FileServer...");
	   server.serve();

  } catch (Exception e) {
   e.printStackTrace();
  }
 }

 public static void main(String[] args) {
   //int mode = -1;
	 nodePort = randInt(9000, 9080);
   if(args.length != 0)
   {
     //mode = Integer.parseInt(args[0]);
	   System.out.println(args[0]);
	   if(args[0].equals("coordinator")){
		   isCoordinator = true;
		   System.out.println("I am a coordinator");
	   }
   }
   //System.out.println("My name is"+nodeName);
  StartsimpleServer(new FileService.Processor<FileServiceHandler>(new FileServiceHandler(isCoordinator,getHostAddress(),nodePort)));
 }

 private static String getHostAddress(){
	 try {
		   InetAddress addr = InetAddress.getLocalHost();
		   nodeName = addr.getHostAddress();
		   	return (nodeName);
		 } catch (UnknownHostException e) {
			 return null;
		 }
 }



}

