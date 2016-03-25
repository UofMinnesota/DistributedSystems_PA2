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
static boolean USE_LOCAL = false;
 public static void StartsimpleServer(FileService.Processor<FileServiceHandler> processor) {
  try {

      int nodePort = randInt(9000, 9080);
   TServerTransport serverTransport = new TServerSocket(nodePort);
   TServer server = new TSimpleServer(
     new Args(serverTransport).processor(processor));

   System.out.println("Establishing connection with the SuperNode...");

   TTransport SuperNodeTransport;
   String supernodeAddr = "csel-x29-10";
   if(USE_LOCAL) supernodeAddr = "localhost";
   SuperNodeTransport = new TSocket(supernodeAddr, 9090); // csel-x29-10
   SuperNodeTransport.open();

   TProtocol SuperNodeProtocol = new TBinaryProtocol(SuperNodeTransport);
   SuperNodeService.Client supernodeclient = new SuperNodeService.Client(SuperNodeProtocol);

  System.out.println("Requesting SuperNode for joining Replica Network through Join Call...");
  
  if(supernodeclient.Join(getHostAddress(),nodePort)){
  System.out.println("The returned list is "+ supernodeclient.GetNodeList());
  }

  else{
	   System.out.println("Supernode busy...");
      }
	   
    System.out.println("Joining DHT...");

	   ArrayList<NodeName> ListOfNodes = new ArrayList<NodeName>();
	   NodeName myName;


       //send DHTList string to the nodeservicehandler
       myName = FileServiceHandler.getMyName();
       ListOfNodes = FileServiceHandler.getListOfNodes();


	   SuperNodeTransport.close();

	   System.out.println("Successfully joined DHT...");
	   System.out.println("Starting simple NodeServer...");
	   server.serve();

  } catch (Exception e) {
   e.printStackTrace();
  }
 }

 public static void main(String[] args) {
   int mode = -1;
   if(args.length != 0)
   {
     mode = Integer.parseInt(args[0]);
   }
  StartsimpleServer(new FileService.Processor<FileServiceHandler>(new FileServiceHandler(mode)));
 }

 private static String getHostAddress(){
	 try {
		   InetAddress addr = InetAddress.getLocalHost();
		   	return (addr.getHostAddress());
		 } catch (UnknownHostException e) {
			 return null;
		 }
 }



}

