package project2;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.TException;
import java.util.ArrayList;
import java.util.Random;

public class FilesClient {
  static boolean USE_LOCAL = false;

  public static class  NodeInfo implements Comparable<NodeInfo>{
    String address = "";
    int port = 0;
    int hash = 0;


    public int compareTo(NodeInfo N) {
        return Integer.compare(hash, N.hash);
    }


  }

  private static ArrayList<NodeName> ListOfNodes = new ArrayList<NodeName>();

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

 public static void main(String[] args) {

   int mode = 0;
   String FileName="";
   String Contents="";
   String ReadInfo="";
   if(args.length != 0)
   {
     mode = Integer.parseInt(args[0]);
     FileName = args[1];
     if(mode == 0){
     Contents = args[2];}
   }
   else{
	   System.out.println("Files Client has to be run with filename/Contents.");
	   System.out.println("Usage: ");
	   System.out.println("Read: <client> <1> <filename> ");
	   System.out.println("Write: <client> <0> <filename> <Contents>");
   }
   
   if(mode == 0){
	   writeFile(FileName,Contents);
   }
   
   if(mode == 1){
	   readFile(FileName);
//	   if(ReadInfo.equals("File not Found..")){
//		   System.out.println("File "+ FileName+ " is not found in the server...");
//	   }
//	   else{
//		   System.out.println("File "+ FileName+ " is found in the server...");
//		   System.out.println("The contents of "+ FileName+ " is ...\n"+ReadInfo);
//	   }
   }
 }


 // Method for getting host address
 private static String getHostAddress(){
	 try {
		   InetAddress addr = InetAddress.getLocalHost();
		   	return (addr.getHostAddress());
		 } catch (UnknownHostException e) {
			 return null;
		 }
 }

//RMI Method for writing files

 public static void writeFile(String FileName, String Contents){

	  try {

		   TTransport SuperNodeTransport;


		   String supernodeAddr = "csel-x32-10";
		   if(USE_LOCAL) supernodeAddr = "localhost";
		   SuperNodeTransport = new TSocket(supernodeAddr, 9090); // csel-x29-10
		   SuperNodeTransport.open();



		   TProtocol SuperNodeProtocol = new TBinaryProtocol(SuperNodeTransport);
		   SuperNodeService.Client supernodeclient = new SuperNodeService.Client(SuperNodeProtocol);
		   ListOfNodes = strToNodeNameArray(supernodeclient.GetNodeList());
		   SuperNodeTransport.close();
		   
		   for(int i=0;i<ListOfNodes.size();i++){
		   System.out.println("Nodes "+ ListOfNodes.get(i).getIP());
		   }
		   
		   
		   int NodeID = randInt(0,ListOfNodes.size());
		   NodeName CurrentNode = ListOfNodes.get(NodeID);
		   
		   System.out.println("Randomly picked node is "+ListOfNodes.get(NodeID).getIP() );
		   


		   TTransport NodeTransport;
		   System.out.println("Connecting to: " + CurrentNode.getIP() + ":" + CurrentNode.getPort()+":"+ CurrentNode.getID());
		   NodeTransport = new TSocket(CurrentNode.getIP(), CurrentNode.getPort());
		   NodeTransport.open();

		   TProtocol NodeProtocol = new TBinaryProtocol(NodeTransport);
		   FileService.Client nodeclient = new FileService.Client(NodeProtocol);

		   if(nodeclient.clientWrite(FileName, Contents) == true){
			  System.out.println("write to File "+ FileName+" has been pushed...");
		   }
		   else{
			   System.out.println("write to File "+ FileName+" NOT successful...");
		   }

		   NodeTransport.close();


		  }
		  catch (TException x) {
		   x.printStackTrace();
		  }

 }


// RMI Method for reading files
public static void readFile(String FileName){

	try {
		   TTransport SuperNodeTransport;

		   String supernodeAddr = "csel-x32-10";
		   if(USE_LOCAL) supernodeAddr = "localhost";
		   SuperNodeTransport = new TSocket(supernodeAddr, 9090); // csel-x29-10
		   SuperNodeTransport.open();

		   TProtocol SuperNodeProtocol = new TBinaryProtocol(SuperNodeTransport);
		   SuperNodeService.Client supernodeclient = new SuperNodeService.Client(SuperNodeProtocol);
		   ListOfNodes = strToNodeNameArray(supernodeclient.GetNodeList());
		   SuperNodeTransport.close();
		   
		   
		   int NodeID = randInt(0,ListOfNodes.size());
		   NodeName CurrentNode = ListOfNodes.get(NodeID);
		   
		   System.out.println("Randomly picked node for read is "+ListOfNodes.get(NodeID).getIP() );
		   

		   TTransport NodeTransport;
		   System.out.println("Connecting to: " + CurrentNode.getIP() + ":" + CurrentNode.getPort()+":"+ CurrentNode.getID());
		   NodeTransport = new TSocket(CurrentNode.getIP(), CurrentNode.getPort());
		   NodeTransport.open();

		   TProtocol NodeProtocol = new TBinaryProtocol(NodeTransport);
		   FileService.Client nodeclient = new FileService.Client(NodeProtocol);
		   
		   String Contents = nodeclient.clientRead(FileName);
		   System.out.println("Contents of File "+ FileName+" is ...");
		   System.out.println(Contents);
			   

			   NodeTransport.close();
		  }
		  catch (TException x) {
		   x.printStackTrace();
		  }
		 }


public static int randInt(int min, int max) {


	  Random rand = new Random();
	  int randomNum = rand.nextInt((max - min)) + min;

	  return randomNum;
	}
}
