package project2;

import java.net.InetAddress;
import java.net.UnknownHostException;

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
import org.apache.thrift.TException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

public class SuperNodeServiceHandler implements SuperNodeService.Iface {

  public class  NodeInfo{
    String address = "";
    int port = 0;
  }

  ArrayList<NodeInfo> ListOfNodes = new ArrayList<NodeInfo>();
  boolean isBusy = false;
  private Random randomGenerator = new Random();
  private static int max_keys = 16;
  private int num_keys = max_keys;
  ArrayList<Integer> ListOfID = new ArrayList<Integer>();
  private static boolean CoordinatorAvailable = false;
  private static boolean CoordinatorChanged = false;
  private static NodeInfo CoordinatorName;
  private static String CoordinatorNameString;
  



  
  //TODO redefine constructor later
  public SuperNodeServiceHandler()
  {
  
  
  }

 @Override
 public String Join(String IP, int Port, boolean isCoordinator) throws TException {



	 System.out.println("Node "+ IP+" : "+Port+" requests for joining Replica Network...");
	 
  
    String NodeList = " ";

	   NodeInfo newNode = new NodeInfo();
	   newNode.address = IP;
	   newNode.port = Port;
	   
	   if(isCoordinator){
		   System.out.println("Setting coordinator as "+IP+":"+Port+" ...");
		   CoordinatorAvailable = true;
		   CoordinatorChanged = true;
		   CoordinatorName = newNode;
		   CoordinatorNameString =IP+":"+Integer.toString(Port);
		   System.out.println("Coordinator Name is "+CoordinatorNameString);
		   //return GetNodeList();
	   }
	   
	   boolean presentFlag=false;
	   for(int i=0;i<ListOfNodes.size();i++){
		   if(ListOfNodes.get(i).address.equals(IP) && ListOfNodes.get(i).port == Port){
			   presentFlag=true;
			   break;
		   }
	   }
	   
	   if(!presentFlag){
	   ListOfNodes.add(newNode);
	   }
	   
	   if(CoordinatorAvailable){
		   System.out.println("Updating nodelist to coordinator "+ IP+" : "+Port+" ..."+ "Node list is "+ GetNodeList());
		  
	   }
	   
		if (CoordinatorChanged) {
			System.out.println("Updating Coordinator information to replicas...");

			for (int x = 0; x < ListOfNodes.size(); x++) {
				if (!((CoordinatorName.address.equals(ListOfNodes.get(x).address)) && (CoordinatorName.port == ListOfNodes.get(x).port))&& 
					  !((newNode.address.equals(ListOfNodes.get(x).address)) && (newNode.port == ListOfNodes.get(x).port))) {
					try {
						TTransport NodeTransport;
						System.out.println("Connecting to: " + ListOfNodes.get(x).address + ":" + ListOfNodes.get(x).port);
						NodeTransport = new TSocket(ListOfNodes.get(x).address,ListOfNodes.get(x).port);
						NodeTransport.open();

						TProtocol NodeProtocol = new TBinaryProtocol(NodeTransport);
						FileService.Client nodeclient = new FileService.Client(
								NodeProtocol);

						nodeclient
								.setCoordinatorInfo(ListOfNodes.get(x).address + ":" + String.valueOf(ListOfNodes.get(x).port));
						NodeTransport.close();
					} catch (TException xx) {
						xx.printStackTrace();
					}
				}
			}
		}
	   
	  
	  
	  System.out.println("Fileserver "+ IP+" : "+Port+" joined  Replica Network...");
	  
	  if(isCoordinator){
		   return GetNodeList();
	   }
	  else if(CoordinatorAvailable){
		  
		  try {
				TTransport NodeTransport;
				System.out.println("Connecting to Coordinator: " + CoordinatorName.address + ":" + CoordinatorName.port);
				NodeTransport = new TSocket(CoordinatorName.address,CoordinatorName.port);
				NodeTransport.open();

				TProtocol NodeProtocol = new TBinaryProtocol(NodeTransport);
				FileService.Client nodeclient = new FileService.Client(
						NodeProtocol);

				//nodeclient.setCoordinatorInfo(ListOfNodes.get(x).address + ":" + String.valueOf(ListOfNodes.get(x).port));
				nodeclient.makeCoordinator(GetNodeList());
				NodeTransport.close();
			} catch (TException xx) {
				xx.printStackTrace();
			}
		  
		  
		  return CoordinatorNameString;
	  }
	  else{
		  return "done";
	  }
 
 }

 @Override
 public String GetNodeList() throws TException {
	 
	 String NodeList = " ";

  for(int x = 0; x < ListOfNodes.size(); x++)
	  {
		  NodeList += ListOfNodes.get(x).address + ":" + String.valueOf(ListOfNodes.get(x).port) +  ",";
	  }

  return NodeList;


 }



}
