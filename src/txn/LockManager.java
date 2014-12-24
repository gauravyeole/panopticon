/**
 * 
 */
package txn;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.json.JSONArray;

import seref.PAKey;
import seref.Vertex;

/**
 * @author aeyate
 *
 */
public abstract class LockManager {
	
	public int localTxnCtr=0;
	public static boolean useML=false;

	public abstract void listenRequests();

	public abstract HashSet<Integer> lockAll(ArrayList<Integer> idList);
	public abstract void unlockAll(ArrayList<Integer> idList);
	public abstract void cleanup();
	
	public static long startTime = System.nanoTime();
	public static boolean zeroMQ = true;
	public HashMap<Integer, Vertex> vertexCache = new HashMap<Integer, Vertex>();
	public HashMap<Integer, Vertex> lazyCache = new HashMap<Integer, Vertex>();
	public HashMap<Integer, Vertex> newLazyCache = new HashMap<Integer, Vertex>();
	public Vertex sampleVertex=new Vertex();
	public HashMap<Integer, Vertex> txnCache = new HashMap<Integer, Vertex>();

	public void printList(HashMap<? extends Object, ? extends Object> mymap){
//		System.out.println(mymap.toString());
		for(Object item : mymap.keySet()){
			System.out.print(item+"\b"+mymap.get(item)+"\t");
		}
	}
	
	public double getTime(){
		return (System.nanoTime()-startTime)/1000000.0;
	}
	
	public HashSet<Integer> convertToInteger(HashSet<String> msgSetStr) {
		HashSet<Integer> msgSet = new HashSet<Integer>();
		for(String vertexID:msgSetStr){
			Integer vid = Integer.parseInt(vertexID);
			msgSet.add(vid);
		}
		return msgSet;
	}

	public void prepareNewLazyCache(ArrayList<Integer> idList){

		newLazyCache.clear();
		for(int i=0; i<idList.size(); i++){
			int nID = idList.get(i);
			Vertex v = lazyCache.get(nID);
			if(v!=null){
				newLazyCache.put(nID, v);
			}
		}
		lazyCache.clear();
	}

	/**
	 * @return
	 */
	public HashMap<Integer, Integer> getLockMap() {
		// TODO Auto-generated method stub
		return null;
	}


}
