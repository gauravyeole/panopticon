/**
 * 
 */
package txn;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;

/**
 * @author aeyate
 *
 */
public class NeighborSelector {
	
	/**
	 * Divides all vertices to wNum-1 groups. Every worker has a different main subset
	 * Each worker selects from its subset a high number of nodes and just a few from the rest.
	 * @return Sorted list of selected vertices
	 */
	public static ArrayList<Integer> selectWeighted(int nodeCount, int wNum, int myID, int NLSIZE){
		ArrayList<Integer> nList = new ArrayList<Integer>(NLSIZE);
		ArrayList<Integer> list2 = new ArrayList<Integer>(nodeCount);
		ArrayList<Integer> myList = new ArrayList<Integer>(nodeCount/(wNum));
		ArrayList<Integer> otherList = new ArrayList<Integer>(nodeCount - nodeCount/(wNum));

		for(int i=0; i<nodeCount; i++){
			list2.add(i);
			if(i >= (myID) * nodeCount/(wNum) && i < (myID+1) * nodeCount/(wNum)){
				myList.add(i);
			}else{
				otherList.add(i);
			}
		}
		Collections.shuffle(list2);
		Collections.shuffle(myList);
		Collections.shuffle(otherList);

		for(int i=0; i<NLSIZE; i++){
			//nList.add(list2.get(i));
			if(i<NLSIZE-2 && i<myList.size()){
				nList.add(myList.get(i));
			}else{
				nList.add(otherList.get(i));
			}
		}
		
		Collections.sort(nList);
		return nList;
	}

	/**
	 * Can select randomly any vertex
	 * @return Sorted list of selected vertices
	 */
	public static ArrayList<Integer> selectRandom(int nodeCount, int wNum, int myID, int NLSIZE){
		ArrayList<Integer> nList = new ArrayList<Integer>(NLSIZE);
		ArrayList<Integer> list2 = new ArrayList<Integer>(nodeCount);

		for(int i=0; i<nodeCount; i++){
			list2.add(i);
		}
		Collections.shuffle(list2);

		for(int i=0; i<NLSIZE; i++){
			nList.add(list2.get(i));
		}
		
		Collections.sort(nList);
		return nList;
	}
	public static ArrayList<Integer> selectHistorical(int nodeCount, int wNum, int myID, int NLSIZE,
			ArrayList<Integer> nList, double histProb){
		ArrayList<Integer> nListCopy = null;

		if(nList == null){
//			nListCopy = selectWeighted(nodeCount, wNum, myID, NLSIZE);
			nListCopy = selectRandom(nodeCount, wNum, myID, NLSIZE);
		}
		else{
			nListCopy = new ArrayList<Integer>(nList);
			for (int i = 0; i < nListCopy.size(); i++) {
				double rnd = (new Random()).nextDouble();
				if(rnd<=histProb){
					nListCopy.set(i, nList.get(i));
				}else{
					nListCopy.set(i, null);
				}
			}
			for (int i = 0; i < nListCopy.size(); i++) {
				if(nListCopy.get(i)!=null) continue;
				int newVal = (new Random()).nextInt(nodeCount);
				while(nListCopy.contains(newVal)){
					newVal = (new Random()).nextInt(nodeCount);
				}
				nListCopy.set(i, newVal);
			}
		}
		
		nList = nListCopy;
		Collections.sort(nList);
		return nList;
	}

	/**
	 * Used for ML. This method must init predictionMap and then select the next item from predictionMap with %hIST_PROB
	 * probability and randomly otherwise. Be careful not to insert any duplicate. Use this for ML experiments.
	 * @param nodeCount
	 * @param numWorkers
	 * @param myID
	 * @param NLSIZE
	 * @param nList the previous txn item list
	 * @param HIST_PROB probability of selecting the item in predictionMap
	 * @return
	 */
	public static ArrayList<Integer> selectML(int nodeCount, int numWorkers,
			Integer myID, int NLSIZE, ArrayList<Integer> nList, double HIST_PROB) {
		ArrayList<Integer> nListCopy = null;

		if(nList == null){
//			nListCopy = selectWeighted(nodeCount, wNum, myID, NLSIZE);
			nListCopy = selectRandom(nodeCount, numWorkers, myID, NLSIZE);
		}
		else{
			nListCopy = new ArrayList<Integer>(nList);
			for (int i = 0; i < nListCopy.size(); i++) {
				double rnd = (new Random()).nextDouble();
				if(rnd<=HIST_PROB){
					nListCopy.set(i, predictionMap.get(nList.get(i)));
//					System.out.println(i + " " + predictionMap.get(i));
				}else{
					nListCopy.set(i, null);
				}
			}
			for (int i = 0; i < nListCopy.size(); i++) {
				if(nListCopy.get(i)!=null) continue;
				int newVal = (new Random()).nextInt(nodeCount);
				while(nListCopy.contains(newVal)){
					newVal = (new Random()).nextInt(nodeCount);
				}
				nListCopy.set(i, newVal);
			}		
		}
		nList = nListCopy;
		Collections.sort(nList);
		return nList;
	}
	
	private static HashMap<Integer,Integer> predictionMap = null;
	public static void initPrMap(int nodeCount){
		// For every item, put n+93th item as the next item.
		predictionMap = new HashMap<Integer,Integer>();
		for (int i=0;i<nodeCount;i++){
			predictionMap.put(i, (i+93)%nodeCount);
		}
	}


}
