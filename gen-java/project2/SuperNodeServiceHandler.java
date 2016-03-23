package project2;

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



  
  //TODO redefine constructor later
  public SuperNodeServiceHandler()
  {
  
  
  }

 @Override
 public String Join(String IP, int Port) throws TException {



	 System.out.println("Node "+ IP+" : "+Port+" requests for joining Replica Network...");
  if(isBusy)
  {
    return "NACK";
  }
  String NodeList = " ";

	   NodeInfo newNode = new NodeInfo();
	   newNode.address = IP;
	   newNode.port = Port;

	   ListOfNodes.add(newNode);

	  for(int x = 0; x < ListOfNodes.size(); x++)
	  {
		  NodeList += ListOfNodes.get(x).address + ":" + String.valueOf(ListOfNodes.get(x).port) +  ",";
	  }

	  isBusy = true;
	  System.out.println("Fileserver "+ IP+" : "+Port+" joined  Replica Network...");
	  return NodeList;
 
 }

 @Override
 public String GetNode() throws TException {
  int x = randomGenerator.nextInt(ListOfNodes.size());
  return  ListOfNodes.get(x).address + ":" + String.valueOf(ListOfNodes.get(x).port) ;

 }

 @Override
 public boolean PostJoin(String IP, int Port) throws TException {

   isBusy = false;

   return isBusy;
 }


}
