package project2;

import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;

public class SuperNode {

 public static void StartsimpleServer(SuperNodeService.Processor<SuperNodeServiceHandler> processor) {
  try {
   TServerTransport serverTransport = new TServerSocket(9090);
   TServer server = new TSimpleServer(
     new Args(serverTransport).processor(processor));

   System.out.println("Starting SuperNode...");
   server.serve();
  } catch (Exception e) {
   e.printStackTrace();
  }
 }

 public static void main(String[] args) {
  StartsimpleServer(new SuperNodeService.Processor<SuperNodeServiceHandler>(new SuperNodeServiceHandler()));
 }

}
