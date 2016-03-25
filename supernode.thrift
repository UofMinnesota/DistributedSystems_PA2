namespace java project2  // defines the namespace   
      
    typedef i32 int  //typedefs to get convenient names for your types  
      
    service SuperNodeService {  // defines the service to add two numbers  
            bool Join(1:string IP, 2:int Port), //defines a method  
            string GetNodeList(), 
    }  
