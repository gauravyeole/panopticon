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
import java.util.Set;

import org.apache.log4j.Logger;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;

/**
 * @author aeyate
 *
 */
public class MasterLockManagerOrdered extends LockManager {

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

	private Logger logger;
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

	public MasterLockManagerOrdered(HazelcastInstance instance, Integer myID, Logger logger, Integer xConsecutive) {
		this.logger = logger;
		this.xConsecutive = xConsecutive;
		this.instance = instance;
		this.workerID = myID;
		inTopic = instance.getTopic("toMaster");
		listenRequests();

	}

	public HashMap<Integer, Integer> getLockMap() {
		return lockMap;
	}
	public HashMap<Integer, Boolean> getisLockedMap() {
		return null;
	}

	@Override
	public void cleanup(){
		logger.debug("totalMessages: "+totalMessages);
		System.out.println("totalMessages: "+totalMessages);
	}


	public void listenRequests(){
		inTopic.addMessageListener(new MessageListener<String>() {
			public void onMessage(Message<String> msg) {
				totalMessages++;
//				(new SntpClient()).printNTPTime(logger);
				final String msgo = msg.getMessageObject();
				logger.debug("lockMap: "+lockMap);

				logger.info(getTime()+" Message received: "+msgo);
				String[] parts = msg.getMessageObject().split("_");
				Integer wid = Integer.parseInt(parts[2]);
				if(!outTopics.containsKey(wid)){
					ITopic<String> outTopic = instance.getTopic("fromMasterTo"+wid);
					outTopics.put(wid, outTopic);
				}
				HashSet<String> msgSetStr=new HashSet<String>(Arrays.asList(parts).subList(3, parts.length));
				HashSet<Integer> msgSet = convertToInteger(msgSetStr);
				HashSet<Integer> txnReqSet = null;


				if(msg.getMessageObject().startsWith("beginTxn_")){
					txnReqSet = msgSet;
					txnRequestMap.put(wid, txnReqSet);
					logger.debug(getTime()+" txnRequestMap: "+txnRequestMap);
					if(amIFirst(wid,txnReqSet)){
						logger.debug(getTime()+" I am the first: "+wid);
//						updateLeases(wid,msgSet);
						handleTxn(wid,txnReqSet);
					}

				}else if(msg.getMessageObject().startsWith("endTxn_") 
						|| msg.getMessageObject().startsWith("reply_")
						|| msg.getMessageObject().startsWith("lazy_")){
					
					addLocks(msgSet);

//					logger.debug(getTime()+" txnRequestMap: "+txnRequestMap);
					Iterator<Integer> keyset = txnRequestMap.keySet().iterator();
//					while(keyset.hasNext()){
//						Integer tWid = keyset.next();
//						if(txnRequestMap.get(tWid)!=null && !txnRequestMap.get(tWid).isEmpty()){ 
//							if(amIFirst(tWid,msgSet) && checkLocks(tWid)){
//								giveLocks(tWid);
//								keyset = txnRequestMap.keySet().iterator();
//							}
//						}
//					}
					
					
					logger.debug("txnRequestMap: "+txnRequestMap);
					keyset = txnRequestMap.keySet().iterator();
					while(keyset.hasNext()){
						Integer tWid = keyset.next();
						txnReqSet=txnRequestMap.get(tWid);
						if(amIFirst(tWid,txnReqSet)){
							logger.debug("I am the first: "+tWid);
//							updateLeases(tWid,msgSet);
							if(handleTxn(tWid,txnReqSet)){
								keyset = txnRequestMap.keySet().iterator();
							}
						}
					}

				}
				logger.debug(getTime()+" Finished onMessage");
				//				logger.debug("lockMap: "+lockMap);
			}
		});
	}



	protected void addLocks(HashSet<Integer> msgSet) {
		synchronized(lockMap){
			for(Integer vid:msgSet){
				lockMap.put(vid, workerID);
			}
		}		
	}

	public boolean checkLocks(Integer tWid){
		synchronized(lockMap){
			for(Integer vid : txnRequestMap.get(tWid)){
				if(lockMap.get(vid)!=workerID && lockMap.get(vid)!=tWid){
					logger.debug("checkLocks: "+tWid+" does not have all locks");
					return false;
				}
			}
			logger.debug("checkLocks: "+tWid+" has all locks");
			return true;
		}
	}

	public void giveLocks(Integer wid){
		final StringBuilder mess = new StringBuilder("reply_"+(++msgCtr)+"_"+wid);
		synchronized(lockMap){
			for(Integer vid : txnRequestMap.get(wid)){
				if(lockMap.get(vid)!=wid){
					lockMap.put(vid, wid);
					mess.append("_").append(vid);
				}
			}
			txnRequestMap.remove(wid);
		}
		publish(mess.toString(), outTopics.get(wid));
	}

	protected boolean handleTxn(Integer wid, HashSet<Integer> msgSet) {

		boolean iHaveAll = createRequests(wid,msgSet);
		if(iHaveAll){
			updateLeases(wid,msgSet);
			giveLocks(wid);
		}
		else{
			for(Integer tWid : futureRequestMap.keySet()){
				if(futureRequestMap.get(tWid).size()>0)
					requestLocks(tWid);
			}
		}
		return iHaveAll;
	}

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
		logger.debug(getTime() + " futureRequestMap: "+futureRequestMap);
		return iHaveAll;
	}

	private synchronized void updateLeases(Integer wid, HashSet<Integer> msgSet) {
		HashSet<Integer> leaseSet = new HashSet<Integer>();
		for(Integer vid:msgSet){
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
			publish(mess.toString(), outTopics.get(wid));
		}

		logger.debug(getTime() + " Updated leases completed.");
		logger.debug("leaseOwnerMap: "+leaseOwnerMap);
		logger.debug("reqHistCounter: "+reqHistCounter);
	}


	public void requestLocks(Integer tWid){
		final StringBuilder mess = new StringBuilder("request_"+(++msgCtr)+"_"+tWid);
		synchronized(lockMap){
			for(Integer vid : futureRequestMap.get(tWid)){
				mess.append("_").append(vid);
			}
		}
		publish(mess.toString(), outTopics.get(tWid));
	}

	public synchronized void publish(final String msgo, final ITopic<String> outTopic){
		totalMessages++;
		logger.info(getTime() + ": Sending message: "+ msgo);
//		(new SntpClient()).printNTPTime(logger);
		outTopic.publish(msgo);
	}



	@Override
	public void unlockAll(ArrayList<Integer> idList) {}
	public enum LeaseType{PERCENTAGE, TWO_CONSECUTIVE}

	@Override
	public HashSet<Integer> lockAll(ArrayList<Integer> idList) {
		return null;
	};
}
