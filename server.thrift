namespace java project2  // defines the namespace   
      
    typedef i32 int  //typedefs to get convenient names for your types  
      
    service FileService {    
            //Interface for server from client
            bool clientWrite(1:string Filename 2:string Contents), //  
            string clientRead(1:string Filename), //defines a method  

            //Interface for Coordinator
            //RPC for making a server as coordinator
            bool makeCoordinator(1:string serverList), 
            
            //update coordinator info to all nodes
            bool setCoordinatorInfo(1:string coordinator),


            //Version numbers:
            //Enquire version number(get)
              int getVersionNumber(1:string Filename), 
            
            //Interface for server from coordinator
            bool serverWrite(1:string Filename 2:string Contents), //  
            string serverRead(1:string Filename), //defines a method

            

    }  
