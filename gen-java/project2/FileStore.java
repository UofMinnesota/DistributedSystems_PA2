package project2;
public class FileStore {
	private String FileName;
	private String Contents;
	private int Version;
	
	FileStore(	String FN, String Cts, int V){
		FileName = FN;
		Contents = Cts;
		Version = V;
	}
	
	public String getFileName(){
		return FileName;
	}
	
	public String getContents(){
		return Contents;
	}
	
	public int getVersion(){
		return Version; 
	}
	
	public void setFileName(String fn){
		FileName = fn;
	}
	
	public void setContents(String cnt){
		Contents = cnt;
	}
	
	public void incrementVersion(){
		Version++; 
	}
	
	public void setVersion(int V){
		Version = V; 
	}
	
}
