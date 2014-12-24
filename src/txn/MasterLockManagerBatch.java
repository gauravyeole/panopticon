/**
 * 
 */
package txn;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;

/**
 * @author aeyate
 *
 */
public class MasterLockManagerBatch extends LockManager {

	private HashMap<Integer, LinkedHashSet<Integer>> requestMap = 
			new HashMap<Integer, LinkedHashSet<Integer>>();
	private HashMap<Integer, Integer> leaseOwnerMap = new HashMap<Integer, Integer>();
	private HashMap<Integer, Integer> lockMap = new HashMap<Integer, Integer>();
	// keeps the number consecutive requests for each vertex
	private HashMap<Integer, ReqTuple> reqHistCounter = new HashMap<Integer, ReqTuple>();

	private HashMap<Integer, ITopic<String>> outTopics = new HashMap<Integer, ITopic<String>>();

	private HazelcastInstance instance;
	private ITopic<String> inTopic, outTopic;
	private Integer workerID;
	private static int mctr=0;
	private static volatile Integer msgCtr = 0;
	private static LeaseType leaseType = LeaseType.TWO_CONSECUTIVE;

	private Logger logger;
	private Integer xConsecutive = 2;
	private volatile Integer reqCount = 0;
	private volatile Integer rereq_ctr = 0;
	private Object lockObject = new Object();
	private final ILock mylock;
	
	private final boolean isAsync = false;
	private final boolean isBatch = true;


    private static ExecutorService executors = Executors.newFixedThreadPool(100);

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

	public MasterLockManagerBatch(HazelcastInstance instance, Integer myID, Logger logger, Integer xConsecutive) {
		this.logger = logger;
		this.xConsecutive = xConsecutive;
		this.instance = instance;
		this.workerID = myID;
		inTopic = instance.getTopic("toMaster");
		outTopic = instance.getTopic("fromMasterTo"+myID);
		mylock=instance.getLock("topicLocker");
		if(isBatch){
			listenRequestsBatch();
		}else{
			listenRequests();
		}
		

	}

	public HashMap<Integer, Integer> getLockMap() {
		return lockMap;
	}
	public HashMap<Integer, Boolean> getisLockedMap() {
		return null;
	}
	
	@Override
	public void cleanup(){
		logger.info("Rerequest count is: "+rereq_ctr);
	}
	
	private HashMap<Integer, LinkedHashSet<Integer>> workerRequestMap = 
			new HashMap<Integer, LinkedHashSet<Integer>>();
	private HashMap<Integer, LinkedHashSet<Integer>> futureRequestMap = 
			new HashMap<Integer, LinkedHashSet<Integer>>();
	
	public void listenRequestsBatch(){
		inTopic.addMessageListener(new MessageListener<String>() {
			public void onMessage(Message<String> msg) {
				mctr++;
				final String msgo = msg.getMessageObject();
				logger.debug("lockMap: "+lockMap);
				logger.debug("workerRequestMap: "+workerRequestMap);
				logger.debug("requestMap: "+requestMap);
				
				logger.info(getTime()+" Message received: "+msgo);
				
				if(mctr%10==0){
					logger.info("Number of items in RequestQueue: "+reqCount);
				}
				String[] parts = msg.getMessageObject().split("_");
				Integer wid = Integer.parseInt(parts[2]);
				if(!outTopics.containsKey(wid)){
					ITopic<String> outTopic = instance.getTopic("fromMasterTo"+wid);
					outTopics.put(wid, outTopic);
				}
				

				if(msgo.startsWith("rerequest_")){
					rereq_ctr++;
					for(int i=3; i<parts.length; i++){
						Integer vid = Integer.parseInt(parts[i]);
						if(lockMap.get(vid)==wid){
							continue;
						}
						if(workerRequestMap.get(wid)==null){
							workerRequestMap.put(wid, new LinkedHashSet<Integer>());
						}else if(workerRequestMap.get(wid).contains(vid)){
//							continue;
						}
						logger.debug("adding rerequest");
						workerRequestMap.get(wid).add(vid);
						logger.debug("added rerequest");
						
						boolean iHaveAll = true;
						for(Integer tvid : workerRequestMap.get(wid)){
							Integer tWid = lockMap.get(tvid);
							if(tWid!=workerID){
								iHaveAll = false;
							}
						}
						if(iHaveAll){
							giveLocks(wid);
						}
						
						synchronized(lockMap){
							Integer ownerID = lockMap.get(vid);
							logger.debug("ownerID "+ownerID);
							if(ownerID != wid && ownerID != workerID){
								String mess = "request_"+(++msgCtr)+"_"+workerID+"_"+vid;						
								logger.info(getTime()+" Sending message: "+ mess);
								publish(mess, outTopics.get(ownerID));
							}
						}
					}
				}
				else if(msgo.startsWith("request_")){
					Integer msgID = Integer.parseInt(parts[1]);
					workerRequestMap.put(wid, new LinkedHashSet<Integer>());
					
					ArrayList<Integer> leaseList = new ArrayList<Integer>();
					logger.info(getTime()+" updateLeases2C started: ");
					for(int i=3; i<parts.length; i++){
						Integer vid = Integer.parseInt(parts[i]);
						boolean justLease = (lockMap.get(vid)==wid);
						if(!justLease) workerRequestMap.get(wid).add(vid);
						
						Integer ret = updateLeases2C(msgID, wid, vid, xConsecutive);
						if(ret!=null)leaseList.add(ret);
						
						if(!justLease && requestMap.get(vid)==null){
							requestMap.put(vid, new LinkedHashSet<Integer>());
						}
						if(!justLease && !requestMap.get(vid).contains(wid)){
							requestMap.get(vid).add(wid);
							synchronized(lockObject){reqCount++;}
						}

					}
					logger.info(getTime()+" updateLeases2C ended: ");
					logger.debug("reqHist: "+reqHistCounter);
					logger.info("leaseMap: "+leaseOwnerMap);

					if(leaseList.size()>0){
						giveLease(wid,leaseList);
					}
					logger.info(getTime()+" giveLease ended: ");

					futureRequestMap = new HashMap<Integer, LinkedHashSet<Integer>>();
					boolean iHaveAll = true;
					for(Integer vid : workerRequestMap.get(wid)){
						Integer tWid = lockMap.get(vid);
						if(tWid!=workerID){
							iHaveAll = false;
							if(futureRequestMap.get(tWid)==null){
								futureRequestMap.put(tWid, new LinkedHashSet<Integer>());
							}
							futureRequestMap.get(tWid).add(vid);
						}
					}
					if(iHaveAll){
						giveLocks(wid);
					}
					else{
						for(Integer tWid : futureRequestMap.keySet()){
							if(futureRequestMap.get(tWid).size()>0)
								requestLocks(tWid);
						}
					}
				}
				else if(msgo.startsWith("reply_")){
					for(int i=3; i<parts.length; i++){
						Integer vid = Integer.parseInt(parts[i]);
						lockMap.put(vid, workerID);
					}
					// burada shuffle ediyoruz ki hep 0 oncelikli olmasin.
					ArrayList<Integer> wList = new ArrayList<Integer>(workerRequestMap.keySet());
					Collections.shuffle(wList);
					for(Integer tWid : wList){
						if(workerRequestMap.get(tWid)!=null && !workerRequestMap.get(tWid).isEmpty()){ 
							if(checkLocks(tWid)){
								giveLocks(tWid);
							}
						}
					}
				}
				logger.debug("lockMap: "+lockMap);
				logger.debug("workerRequestMap: "+workerRequestMap);
				logger.debug("requestMap: "+requestMap);
			}
		});
	}
	
	public boolean checkLocks(Integer tWid){
		for(Integer vid : workerRequestMap.get(tWid)){
			if(lockMap.get(vid)!=workerID){
				return false;
			}
		}
		return true;
	}

	public void requestLocks(Integer tWid){
		final StringBuilder mess = new StringBuilder("request_"+(++msgCtr)+"_"+workerID);
		for(Integer vid : futureRequestMap.get(tWid)){
			mess.append("_").append(vid);
		}
		publish(mess.toString(), outTopics.get(tWid));
	}
	
	public void giveLocks(Integer wid){
		final StringBuilder mess = new StringBuilder("reply_"+(++msgCtr)+"_"+workerID);
		LinkedHashSet<Integer> curList = workerRequestMap.get(wid);
		for(Integer vid : workerRequestMap.get(wid)){
			lockMap.put(vid, wid);
			requestMap.get(vid).remove(wid);
			mess.append("_").append(vid);
		}
		publish(mess.toString(), outTopics.get(wid));
		workerRequestMap.put(wid,null);
		checkIfRequested(wid, curList);
	}

	private void checkIfRequested(Integer wid, LinkedHashSet<Integer> curList) {
		final StringBuilder mess = new StringBuilder("request_"+(++msgCtr)+"_"+workerID);
		boolean isEmpty = true;
		for(Integer tVid : curList){
			if(requestMap.get(tVid)!=null && !requestMap.get(tVid).isEmpty()){ 
				mess.append("_").append(tVid);
				isEmpty = false;
			}
		}
		if(!isEmpty) publish(mess.toString(), outTopics.get(wid));
	}

	public void listenRequests(){
		inTopic.addMessageListener(new MessageListener<String>() {
			public void onMessage(Message<String> msg) {
				mctr++;
				final String msgo = msg.getMessageObject();
				logger.info(getTime()+" Message received: "+msgo);
				
//				logger.debug("requestMap: "+requestMap);
//				logger.debug("lockMap: "+lockMap);
				if(mctr%10==0){
					logger.info("Number of items in RequestQueue: "+reqCount);
				}
				String[] parts = msg.getMessageObject().split("_");
				Integer wid = Integer.parseInt(parts[2]);
				if(!outTopics.containsKey(wid)){
					ITopic<String> outTopic = instance.getTopic("fromMasterTo"+wid);
					outTopics.put(wid, outTopic);
				}

				if(msgo.startsWith("request_") 
						|| msgo.startsWith("rerequest_")){
//					analyzeMessages(msg.getMessageObject());
					Integer msgID = Integer.parseInt(parts[1]);
					
					
					for(int i=3; i<parts.length; i++){
						Integer vid = Integer.parseInt(parts[i]);
						if(!parts[0].equals("rerequest")){
							logger.info(getTime()+" updateLeases2C started: ");
							updateLeases2C(msgID, wid, vid, xConsecutive);
							logger.info(getTime()+" updateLeases2C ended: ");
						}else{
							rereq_ctr++;
						}
						synchronized(lockMap){
							Integer ownerID = lockMap.get(vid);
							if(ownerID==workerID){
								logger.info(getTime()+" Master has the lock started: ");
								lockMap.put(vid, wid);
//								ITopic<String> outTopic = instance.getTopic("fromMasterTo"+wid);
								final String mess = "reply_"+(++msgCtr)+"_"+workerID+"_"+vid;						
//								logger.info(getTime()+" Sending message: "+ mess);
//								outTopic.publish(mess);
								logger.info(getTime()+" Sending message using ExecutorService: "+ mess);
								publish(mess, outTopics.get(wid));
								logger.info(getTime()+" Master has the lock ended: ");
							}else if(ownerID != wid && ownerID > workerID){
								logger.info(getTime()+" Master does not hold the lock started: ");
//								ITopic<String> outTopic = instance.getTopic("fromMasterTo"+ownerID);
								String mess = "request_"+(++msgCtr)+"_"+workerID+"_"+vid;						
								logger.info(getTime()+" Sending message: "+ mess);
//								outTopic.publish(mess);
								publish(mess, outTopics.get(ownerID));
								logger.info(getTime()+" Publishing ended: ");
								if(requestMap.get(vid)==null){
									requestMap.put(vid, new LinkedHashSet<Integer>());
								}
								if(!requestMap.get(vid).contains(wid)){
									requestMap.get(vid).add(wid);
									synchronized(lockObject){reqCount++;}
								}
								logger.info(getTime()+" Master does not hold the lock ended: ");
							}
						}
					}
				}
				else if(msg.getMessageObject().startsWith("reply_")){
					for(int i=3; i<parts.length; i++){
						Integer vid = Integer.parseInt(parts[i]);
						Integer target = null;
						synchronized(lockMap){
							if(requestMap.get(vid)==null){
								requestMap.put(vid, new LinkedHashSet<Integer>());
							}
							Iterator<Integer> it = requestMap.get(vid).iterator();
							if(it.hasNext()){
								target = it.next();
							}
							if(target == null){
								lockMap.put(vid, workerID);
							}
							else{
								lockMap.put(vid, target);
								requestMap.get(vid).remove(target);
								synchronized(lockObject){reqCount--;}
//								ITopic<String> outTopic = instance.getTopic("fromMasterTo"+target);
								String mess = "reply_"+(++msgCtr)+"_"+workerID+"_"+vid;
								logger.info(getTime()+" Sending message: "+ mess);
//								outTopic.publish(mess);
								publish(mess, outTopics.get(target));
							}
						}
					}
				}
				logger.debug("requestMap: "+requestMap);
				logger.debug("lockMap: "+lockMap);
//				logger.debug("statMap: "+statMap);
				
				logger.debug("Finished onMessage");
			}

		});
	}

	private synchronized Integer updateLeases2C(Integer msgID, Integer wid, Integer vid, Integer xConsecutive) {
//		logger.info(getTime()+" updateLeases2C started: ");
		Integer ret=null;
		if(leaseType != LeaseType.TWO_CONSECUTIVE){
			return null;
		}
//		logger.debug("Update lease started: "+msgID+"_"+wid+"_"+vid);
		
		int LIMIT = xConsecutive; // change this to consecutive checking level
//		if(reqCounter.get(wid)==null){
//			reqCounter.put(wid, -1);
//		}
		if(leaseOwnerMap.get(vid)==null){
			leaseOwnerMap.put(vid, workerID);
		}else if(leaseOwnerMap.get(vid)==wid){
			return null;
		}
		
		if(leaseOwnerMap.get(vid)!=wid){
			leaseOwnerMap.put(vid, workerID);
		}
		if(reqHistCounter.get(vid)==null){
			reqHistCounter.put(vid, new ReqTuple(workerID,0));
		}
		
//		if(reqCounter.get(wid)!=msgID){
			if(reqHistCounter.get(vid).wID==wid){
				reqHistCounter.get(vid).count++;
				if(reqHistCounter.get(vid).count >= LIMIT){
					if(!isBatch){ giveLease(wid,vid);}
					else{ ret = vid;}
				}
			}else{
				reqHistCounter.get(vid).count=1;
				reqHistCounter.get(vid).wID=wid;				
			}
//		}
		
//		logger.debug(reqCounter);
//		logger.debug(leaseMap);
//		logger.info(getTime()+" updateLeases2C ended: ");
		
		return ret;

	}
	
	public void publish(final String msgo, final ITopic<String> outTopic){
		logger.info(getTime() + ": Sending message: "+ msgo);
		if(isAsync){
			executors.execute(new Runnable() {
				public void run() {
					mylock.lock();
					outTopic.publish(msgo);
					mylock.unlock();
				}
			});
		}
		else{
			outTopic.publish(msgo);
		}
	}

	public void giveLease(Integer wid, ArrayList<Integer> leaseList){
		StringBuilder mess = new StringBuilder("lease_add");
		boolean isEmpty = true;
		for(Integer vid : leaseList){
			Integer oldOwner = leaseOwnerMap.get(vid);
			if(oldOwner!=wid){
				isEmpty = false;
				mess.append("_").append(vid);
				leaseOwnerMap.put(vid, wid);
			}
		}
		
		if(!isEmpty){ 
			publish(mess.toString(), outTopics.get(wid));
		}
	}

	public void giveLease(Integer wid, Integer vid){
		Integer oldOwner = leaseOwnerMap.get(vid);
		if(oldOwner!=wid){
//			logger.info(getTime()+" giveLease started: ");
//			outTopic = instance.getTopic("fromMasterTo"+wid);
			String mess = "lease_add_"+vid;
			logger.info(getTime()+" LEASE message: "+ mess);
//			outTopic.publish(mess);
			publish(mess, outTopics.get(wid));
//			logger.info(getTime()+" giveLease ended: ");

			if(oldOwner!=workerID){
//				ITopic<String> outTopic = instance.getTopic("fromMasterTo"+oldOwner);
				mess = "lease_remove_"+vid;
				logger.info(getTime()+" LEASE message: "+ mess);
//				outTopic.publish(mess);
				publish(mess, outTopics.get(oldOwner));
			}
			leaseOwnerMap.put(vid, wid);
		}

	}

	@Override
	public void unlockAll(ArrayList<Integer> idList) {
	}
	public enum LeaseType{PERCENTAGE, TWO_CONSECUTIVE}
	/* (non-Javadoc)
	 * @see txn.LockManager#lockAll(java.util.ArrayList)
	 */
	@Override
	public HashSet<Integer> lockAll(ArrayList<Integer> idList) {
		// TODO Auto-generated method stub
		return null;
	};
}
