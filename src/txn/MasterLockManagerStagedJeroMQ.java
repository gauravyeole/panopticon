/**
 * 
 */
package txn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import org.jeromq.ZMQ;
import org.jeromq.ZMQ.Context;
import org.jeromq.ZMQ.Socket;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;

/**
 * @author aeyate
 *
 */
public class MasterLockManagerStagedJeroMQ extends LockManager {

	private HashMap<Integer, Integer> leaseOwnerMap = new HashMap<Integer, Integer>();
	private HashMap<Integer, Integer> lockMap = new HashMap<Integer, Integer>();
	// keeps the number consecutive requests for each vertex
	private HashMap<Integer, ReqTuple> reqHistCounter = new HashMap<Integer, ReqTuple>();

	private HashMap<Integer, ITopic<String>> outTopics = new HashMap<Integer, ITopic<String>>();
	private LinkedHashMap<Integer, HashSet<Integer>> txnRequestMap = 
			new LinkedHashMap<Integer, HashSet<Integer>>();
	private HashMap<Integer, LinkedHashSet<Integer>> futureRequestMap = 
			new HashMap<Integer, LinkedHashSet<Integer>>();


	private HazelcastInstance instance;
	private ITopic<String> inTopic;
	private Integer workerID;
	private static volatile Integer msgCtr = 0;
	private static volatile Integer totalMessages = 0;

	private Logger LOG;
	private Integer xConsecutive = 2;


	public class ReqTuple{
		public ReqTuple(int i, int j) {
			wID = i;
			count = j;
		}
		public Integer count;
		public Integer wID;

		@Override
		public String toString(){
			return wID+":"+count;
		}
	}

	private Object lockObject = new Object();
	private final ILock mylock;
	boolean isAsync=false;
    private static ExecutorService executors = Executors.newFixedThreadPool(32);
	private HashMap<Integer, LinkedBlockingQueue<String>> lbqMap = 
			new HashMap<Integer, LinkedBlockingQueue<String>>();
	private ConcurrentHashMap<Integer,Integer> busySet = new ConcurrentHashMap<Integer,Integer>();
	private HashSet<Integer> requestedBefore = new HashSet<Integer>();

    Context scontext = ZMQ.context(1);
    HashMap<String,Socket> publisher = new HashMap<String,Socket>();
    HashMap<String,Context> pcontext = new HashMap<String,Context>();
    Socket subscriber = scontext.socket(ZMQ.SUB);
    String masterAddr = null;
    private IMap<String, String> ipMap; 
    private HashMap<String, String> localIpMap = new HashMap<String, String>(); 

	public MasterLockManagerStagedJeroMQ(HazelcastInstance instance, Integer myID, Logger logger, 
			Integer xConsecutive, String ip) {
		this.LOG = logger;
		this.xConsecutive = xConsecutive;
		this.instance = instance;
		this.workerID = myID;
		inTopic = instance.getTopic("toMaster");
		mylock=instance.getLock("topicLocker");

		ipMap = instance.getMap("ipMap");
//		String myIP = "127.0.0.1";
		String myIP = ip.split(":")[0];// instance.getCluster().getLocalMember().getInetSocketAddress().getAddress().toString();
		int myPort = 3013;//instance.getConfig().getNetworkConfig().getPort();
		int jmqPort = myPort+200;
		ipMap.put(myID+"", "tcp:/"+myIP+":"+jmqPort); // TODO: Bu degerlerin dogru oldugundan emin ol
		LOG.info("My ip is: "+ipMap.get(myID+""));
		System.out.println("My ip is: "+ipMap.get(myID+""));
		subscriber.bind("tcp://0.0.0.0:"+jmqPort); // TODO: Burda port'u ne secmemiz gerekiyor?
        subscriber.subscribe("toMaster");
        new Thread( new Runnable(){
        	public void run(){
        		listenRequests();
        	}
        }).start();

	}

	public HashMap<Integer, Integer> getLockMap() {
		return lockMap;
	}
	public HashMap<Integer, Boolean> getisLockedMap() {
		return null;
	}

	@Override
	public void cleanup(){
		LOG.debug("totalMessages: "+totalMessages);
		System.out.println("totalMessages: "+totalMessages);
	}


	public void listenRequests(){
//		inTopic.addMessageListener(new MessageListener<String>() {
//			public void onMessage(Message<String> msg) {
		while(true){
				String msg = subscriber.recvStr();
				if(msg.startsWith("to")) continue;
				totalMessages++;
//				(new SntpClient()).printNTPTime(logger);
				final String msgo = msg;
//				LOG.debug("lockMap: "+lockMap);

				LOG.info(getTime()+" Message received: "+msgo);
				String[] parts = msg.split("_");
				Integer wid = Integer.parseInt(parts[2]);
				if(!outTopics.containsKey(wid)){
					ITopic<String> outTopic = instance.getTopic("fromMasterTo"+wid);
					outTopics.put(wid, outTopic);
					lbqMap.put(wid, new LinkedBlockingQueue<String>());
				}
				HashSet<String> msgSetStr=new HashSet<String>(Arrays.asList(parts).subList(3, parts.length));
				HashSet<Integer> msgSet = convertToInteger(msgSetStr);
				HashSet<Integer> txnReqSet = null;


				if(msg.startsWith("beginTxn_")){
					txnReqSet = msgSet;
					txnRequestMap.put(wid, txnReqSet);
					LOG.debug(getTime()+" txnRequestMap: "+txnRequestMap);
					updatePredictionMatrix(wid);
					if(checkLocks(wid)){
						HashSet<Integer> curList = txnRequestMap.remove(wid);
						requestedBefore.remove(wid);
						// Run ML here. 
						prevTxnList.put(wid, curList);
						HashSet<Integer> nextList = predictNextTxn(curList);
						LOG.debug("ML nextList is: "+nextList);
//						System.out.println("useML is: "+LockManager.useML);
						if(useML) giveLocks(wid,nextList, "ml");
						else giveLocks(wid,new HashSet<Integer>(Arrays.asList(-10)),"ml");

						continue;
					}
					if(amIFirst(wid,txnReqSet)){
						LOG.debug(getTime()+" I am the first: "+wid);
//						updateLeases(wid,msgSet);
						handleTxnFast(wid,txnReqSet);
					}

				}else if(msg.startsWith("endTxn_") 
						|| msg.startsWith("reply_")
						|| msg.startsWith("lazy_")){
					
					addLocks(msgSet);
					LOG.debug(getTime()+" Added locks: "+msgSet);
					Iterator<Integer> keyset = txnRequestMap.keySet().iterator();
					LOG.debug(getTime()+" txnRequestMap: "+txnRequestMap);
					while(keyset.hasNext()){
						Integer tWid = keyset.next();
						txnReqSet=txnRequestMap.get(tWid);
						if(amIFirst(tWid,txnReqSet)){
							LOG.debug(getTime()+" I am the first: "+tWid);
//							updateLeases(tWid,msgSet);
							if(handleTxnFast(tWid,txnReqSet)){
								keyset = txnRequestMap.keySet().iterator();
							}
						}
					}

				}
				LOG.info(getTime()+" Finished onMessage");
				//				logger.debug("lockMap: "+lockMap);
			}
//		});
	}


	/**
	 * @param msgSet
	 * Adds lock of items in msgSet to lockMap of broker
	 */
	protected void addLocks(HashSet<Integer> msgSet) {
		synchronized(lockMap){
			for(Integer vid:msgSet){
				lockMap.put(vid, workerID);
			}
		}		
	}

	/**
	 * @param tWid
	 * @return true if tWid has all locks. No side effects
	 */
	public boolean checkLocks(Integer tWid){
		synchronized(lockMap){
			for(Integer vid : txnRequestMap.get(tWid)){
				if(lockMap.get(vid)!=tWid){
					LOG.debug("checkLocks: "+tWid+" does not have all locks");
					return false;
				}
			}
			LOG.debug("checkLocks: "+tWid+" has all locks");
			return true;
		}
	}

	public void giveLocks(Integer wid, HashSet<Integer> itemsToGive, String msg){
		final StringBuilder mess = new StringBuilder(msg+"_"+(++msgCtr)+"_"+wid);
		boolean isEmpty = true;
		synchronized(lockMap){
			for(Integer vid : itemsToGive){
				if(vid == -10){
					isEmpty = false;
					mess.append("_");
					mess.append(vid);					
				}
				else if(lockMap.get(vid)==workerID){
					isEmpty = false;
					lockMap.put(vid, wid);
					mess.append("_");
					if(isAnyoneWaiting(vid,wid)){
						mess.append("+");
					}
					mess.append(vid);
				}
			}
		}
		if(!isEmpty){ 
//			publish(mess.toString(), wid);
			publishZMQ(mess.toString(), wid+"");
		}
	}

	protected boolean handleTxn(Integer wid, HashSet<Integer> msgSet) {

		boolean iHaveAll = createRequests(wid,msgSet);
		if(iHaveAll){
			updateLeases(wid,msgSet);
			giveLocks(wid, txnRequestMap.get(wid), "reply");
			txnRequestMap.remove(wid);
		}
		else{
			for(Integer tWid : futureRequestMap.keySet()){
				if(futureRequestMap.get(tWid).size()>0)
					requestLocks(tWid);
			}
		}
		return iHaveAll;
	}
	
	protected boolean handleTxnFast(Integer wid, HashSet<Integer> msgSet) {
		LOG.debug(getTime() + " requestedBefore: "+requestedBefore);
		updateLeases(wid,msgSet);
		giveLocks(wid, txnRequestMap.get(wid), "reply");

		boolean iHaveAll = createRequests(wid,msgSet);

		if(iHaveAll){
			HashSet<Integer> curList = txnRequestMap.remove(wid);
			requestedBefore.remove(wid);
			// Run ML here. 
			prevTxnList.put(wid, curList);
			HashSet<Integer> nextList = predictNextTxn(curList);
			LOG.debug("ML nextList is: "+nextList);
//			System.out.println("useML is: "+LockManager.useML);
			if(useML) giveLocks(wid,nextList, "ml");
			else giveLocks(wid,new HashSet<Integer>(Arrays.asList(-10)),"ml");
		}
		else{
			if(requestedBefore.contains(wid)){
				return iHaveAll;
			}
			requestedBefore.add(wid);
			for(Integer tWid : futureRequestMap.keySet()){
				if(futureRequestMap.get(tWid).size()>0)
					requestLocks(tWid);
			}
		}
		return iHaveAll;
	}
	
	
	// ----------ML PREDICTION RELATED ITEMS--------------------
	/**
	 * Keeps every worker's item list from the previous txn
	 */
	private LinkedHashMap<Integer, HashSet<Integer>> prevTxnList = 
			new LinkedHashMap<Integer, HashSet<Integer>>();
	
	/**
	 * For every item i, keeps the number of times the item j is requested in the next txn by the same worker
	 * <i,<j,count>>
	 */
	private LinkedHashMap<Integer, HashMap<Integer,Integer>> predictionMatrix = 
			new LinkedHashMap<Integer, HashMap<Integer,Integer>>();
	

	/**
	 * For ML. Checks the current requested items and predicts the items that will be used in the next transaction.
	 * It then checks if isAnyoneWaiting(item) and adds to returned list if false.
	 * @param curList
	 * @return the predicted items which are not being waited by any other worker now.
	 */
	private HashSet<Integer> predictNextTxn(HashSet<Integer> curList) {
		// need to use a map which will keep at (i,j) the number of times j is requested in txn n+1 after i is
		// requested by the same worker in txn n.
		// Using this map, calculate the most probable next item and add it to return list.
		HashSet<Integer> retList = new HashSet<Integer>();
		
		for(Integer curItem : curList){
			retList.add((curItem+93)%lockMap.size());
//			int total = 0;
//			int maxID = -1;
//			int maxVal=0;
//			int maxVal2 = 0;
//			if(predictionMatrix.get(curItem)==null){
//				predictionMatrix.put(curItem, new HashMap<Integer,Integer>());
//			}
//			for(Map.Entry<Integer,Integer> nextItem : predictionMatrix.get(curItem).entrySet()){
//				total+=nextItem.getValue();
//				if(nextItem.getValue() > maxVal){
//					maxVal2 = maxVal;
//					maxVal = nextItem.getValue();
//					maxID = nextItem.getKey();
//				}else if(nextItem.getValue() > maxVal2){
//					maxVal2 = nextItem.getValue();
//				}
//			}
////			LOG.debug("Prediction matrix for item "+curItem+" is: "+predictionMatrix.get(curItem));
////			LOG.debug("Total, maxVal/maxVal2, maxID is: "+total+", "+maxVal+"/"+maxVal2+", "+maxID);
//			if(total > 10 && maxVal > 2 && maxVal/maxVal2 >= 2){ // If prob of an element is higher than 50%, give it
//				if(!isAnyoneWaiting(maxID,-1)){
//					retList.add(maxID);
//				}
//			}
		}
		return retList;
	}

	
	/**
	 * Reads the prevTxnList to learn what this wid used in the previous txn and current txn items
	 * from txnRequestMap and then increments the related entries in the predictionMatrix.
	 * @param wid
	 */
	protected void updatePredictionMatrix(Integer wid) {
//		// TODO Auto-generated method stub
//		if(prevTxnList.get(wid)==null){
//			prevTxnList.put(wid, new HashSet<Integer>());
//		}
//		try{
//			HashSet<Integer> prevList = prevTxnList.get(wid);
//			for(Integer prev : prevList){
//				if(predictionMatrix.get(prev)==null){
//					predictionMatrix.put(prev, new HashMap<Integer,Integer>());
//				}
//				for(Integer cur : txnRequestMap.get(wid)){
//					if(predictionMatrix.get(prev).get(cur)==null){
//						predictionMatrix.get(prev).put(cur, 0);
//					}
//					int val = predictionMatrix.get(prev).get(cur)+1;
//					predictionMatrix.get(prev).put(cur, val);
//				}
//			}
//		}catch(Exception e){
//			e.printStackTrace();
//		}
////		LOG.debug("updatePredictionMatrix completed for worker: "+wid);
	}
	// ----------END OF ML PREDICTION RELATED ITEMS--------------------


	/** This function checks if for all the items in my transaction request, 
	 * i am the first in the request queue.
	 * 
	 * @param wid	id of the worker
	 * @param msgSet	request list of the latest transaction from worker wid
	 * @return	true if none of my msgSet is in reqSet of another worker before me
	 */
	private boolean amIFirst(Integer wid, HashSet<Integer> msgSet) {
		synchronized(lockMap){
			for(Integer vid : msgSet){
				for(Integer twid : txnRequestMap.keySet()){
					if(wid==twid) break;
					if(txnRequestMap.get(twid).contains(vid)){
						return false;
					}
				}
			}
		}
		return true;
	}
	
	private boolean isAnyoneWaiting(Integer vid, Integer wid) {
		synchronized(lockMap){
				for(Integer twid : txnRequestMap.keySet()){
					if(wid==twid) continue;
					if(txnRequestMap.get(twid).contains(vid)){
						return true;
					}
				}
		}
		return false;
	}

	/** Fills futureRequestMap based on msgSet. Checks the lockMap and 
	 * if the wid or master is not the owner of the lock, then adds the vertex id
	 * to the requestMap for curent lock owner. If all is owned by master or the requesting
	 * worker, then it returns iHaveAll as true.
	 * 
	 * @param wid
	 * @param msgSet
	 * @return	true if all msgSet items are either on master or requesting worker
	 */
	private boolean createRequests(Integer wid, HashSet<Integer> msgSet) {
		boolean iHaveAll = true;
		synchronized(lockMap){
			futureRequestMap = new HashMap<Integer, LinkedHashSet<Integer>>();
			for(Integer vid : msgSet){
				Integer tWid = lockMap.get(vid);
				if(tWid!=wid && tWid!=workerID){
					iHaveAll = false;
					if(futureRequestMap.get(tWid)==null){
						futureRequestMap.put(tWid, new LinkedHashSet<Integer>());
					}
					futureRequestMap.get(tWid).add(vid);
				}
			}
		}
//		LOG.debug(getTime() + " futureRequestMap: "+futureRequestMap);
		return iHaveAll;
	}

	private synchronized void updateLeases(Integer wid, HashSet<Integer> msgSet) {
		HashSet<Integer> leaseSet = new HashSet<Integer>();
		for(Integer vid:msgSet){
			if(lockMap.get(vid)!=workerID){
				continue;
			}
			if(reqHistCounter.get(vid)==null){
				reqHistCounter.put(vid, new ReqTuple(wid,1));
			}else if(reqHistCounter.get(vid).wID==wid){
				reqHistCounter.get(vid).count++;
			}else{
				reqHistCounter.get(vid).count=1;
				reqHistCounter.get(vid).wID=wid;
				leaseOwnerMap.remove(vid);
			}

			if(reqHistCounter.get(vid).count >= xConsecutive && leaseOwnerMap.get(vid) != wid){
				leaseSet.add(vid);
			}
		}

		StringBuilder mess = new StringBuilder("lease_add");
		boolean isEmpty = (leaseSet.size()==0);
		for(Integer vid : leaseSet){
				mess.append("_").append(vid);
				leaseOwnerMap.put(vid, wid);
		}
		if(!isEmpty){ 
//			publish(mess.toString(), wid);
			publishZMQ(mess.toString(), wid+"");
		}

//		LOG.debug(getTime() + " Updated leases completed.");
//		LOG.debug("leaseOwnerMap: "+leaseOwnerMap);
//		LOG.debug("reqHistCounter: "+reqHistCounter);
	}


	public void requestLocks(Integer tWid){
		final StringBuilder mess = new StringBuilder("request_"+(++msgCtr)+"_"+tWid);
		synchronized(lockMap){
			for(Integer vid : futureRequestMap.get(tWid)){
				mess.append("_").append(vid);
			}
		}
//		publish(mess.toString(), tWid);
		publishZMQ(mess.toString(), tWid+"");
	}
	
	public void publishZMQ(final String msgo, String wid){
		totalMessages++;
		LOG.info(getTime() + ": Sending message: "+ msgo);
		if(localIpMap.get(wid)==null){
			localIpMap.put(wid, ipMap.get(wid));
//			LOG.info(localIpMap.get(wid)+"\t"+ipMap.get(wid));
			pcontext.put(wid, ZMQ.context(1));
			publisher.put(wid, pcontext.get(wid).socket(ZMQ.PUSH));
			publisher.get(wid).connect(localIpMap.get(wid));
		}
		publisher.get(wid).sendMore("fromMasterTo"+wid);
        publisher.get(wid).send(msgo);
	}

	public void publish(final String msgo, final Integer tWid){
		final ITopic<String> outTopic = outTopics.get(tWid);
		totalMessages++;
		LOG.info(getTime() + ": Sending message: "+ msgo);

		if(isAsync){
			try {
				lbqMap.get(tWid).put(msgo);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			if(!busySet.containsKey(tWid)){
				busySet.put(tWid,null);
				executors.execute(new Runnable(){
					public void run() {
						synchronized(lbqMap.get(tWid)){
							while(!lbqMap.get(tWid).isEmpty()){
								String mess=lbqMap.get(tWid).poll();
								outTopic.publish(mess);
							}	
						}
						busySet.remove(tWid);
					}
				});
			}
		}
		else{
			outTopic.publish(msgo);
		}
	}



	@Override
	public void unlockAll(ArrayList<Integer> idList) {}
	public enum LeaseType{PERCENTAGE, TWO_CONSECUTIVE}

	@Override
	public HashSet<Integer> lockAll(ArrayList<Integer> idList) {
		return null;
	};
}
