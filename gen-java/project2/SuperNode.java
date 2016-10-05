package project2;

import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;

public class SuperNode {

  public static SuperNodeServiceHandler handler;

   public static SuperNodeService.Processor processor;


 public static void StartsimpleServer(SuperNodeService.Processor<SuperNodeServiceHandler> processor, int port) {
  try {
   TServerTransport serverTransport = new TServerSocket(port);
   TServer server = new TSimpleServer(
     new Args(serverTransport).processor(processor));

   System.out.println("Starting SuperNode..." + port);
   server.serve();
  } catch (Exception e) {
   e.printStackTrace();
  }
 }



 public static void main(String[] args) {
  //StartsimpleServer(new SuperNodeService.Processor<SuperNodeServiceHandler>(new SuperNodeServiceHandler()));



    try {
        handler = new SuperNodeServiceHandler();
        processor = new SuperNodeService.Processor(handler);

        Runnable simple = new Runnable() {
          public void run() {
            StartsimpleServer(processor, 9090);
          }
        };

        new Thread(simple).start();
      } catch (Exception x) {
        x.printStackTrace();
      }

      
 }

}
