package project1;

import java.util.ArrayList;

// Class containing members and methods for finger table

public class FingerTable {
		
	private int start;
	private int intervalBegin;
	private int intervalEnd;
	private NodeName successor;
	
	//Setters
		public void setStart(int newStart){
			this.start = newStart;
		}	

		public void setInterval(int begin, int end){
			this.intervalBegin = begin;
			this.intervalEnd = end;
		}

		public void setSuccessor(NodeName newSuccessor){
			this.successor = newSuccessor;
		}

		//Getters
		public int getStart(){
			return this.start;
		}

		public int getIntervalBegin(){
			return this.intervalBegin;
		}

		public int getIntervalEnd(){
			return this.intervalEnd;
		}

		public NodeName getSuccessor(){
			return this.successor;
		}
		
		public void findSuccessor(NodeName N, ArrayList<NodeName> ListOfNodes,int numDHT){
			
			boolean found=false;
			//int numDHT = (int)Math.pow(2,ListOfNodes.size());
			
			System.out.println("NumDHT is "+numDHT);
			
			for(int i=0; i< ListOfNodes.size();i++){
				System.out.println("Node ID is "+ ListOfNodes.get(i).getID()+" My start ID is "+start);
				if(ListOfNodes.get(i).getID() >= start ){
					successor = ListOfNodes.get(i);
					found=true;
					break;
				}
				else{
					System.out.println("Succesor not found ID is "+ ListOfNodes.get(i).getID() );
				}
			}
			
			if(found == false){
				if(start/numDHT > 0){
				  for(int i=0; i< ListOfNodes.size();i++){
					if(ListOfNodes.get(i).getID() >= start%numDHT ){
						successor = ListOfNodes.get(i);
						found=true;
						break;
					}
				  }
				}
				else{
					successor = ListOfNodes.get(0);
					System.out.println("Successor ID is "+successor.getID());
				}
			}
			
		}

		public FingerTable(){
		}

		public FingerTable(int startID, NodeName succ) {
			start = startID;
			successor = succ;
		}
}
