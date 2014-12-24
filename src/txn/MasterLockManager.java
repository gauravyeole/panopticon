/**
 * 
 */
package txn;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
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
public class MasterLockManager extends LockManager {

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

	public MasterLockManager(HazelcastInstance instance, Integer myID, Logger logger, Integer xConsecutive) {
		this.logger = logger;
		this.xConsecutive = xConsecutive;
		this.instance = instance;
		this.workerID = myID;
		inTopic = instance.getTopic("toMaster");
		outTopic = instance.getTopic("fromMasterTo"+myID);
		mylock=instance.getLock("topicLocker");
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
		logger.info("Rerequest count is: "+rereq_ctr);
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

	private synchronized void updateLeases2C(Integer msgID, Integer wid, Integer vid, Integer xConsecutive) {
//		logger.info(getTime()+" updateLeases2C started: ");
		if(leaseType != LeaseType.TWO_CONSECUTIVE){
			return;
		}
		logger.debug("Update lease started: "+msgID+"_"+wid+"_"+vid);
		
		int LIMIT = xConsecutive; // change this to consecutive checking level
//		if(reqCounter.get(wid)==null){
//			reqCounter.put(wid, -1);
//		}
		if(leaseOwnerMap.get(vid)==null){
			leaseOwnerMap.put(vid, workerID);
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
					giveLease(wid,vid);
				}
			}else{
				reqHistCounter.get(vid).count=1;
				reqHistCounter.get(vid).wID=wid;				
			}
//		}
		
//		logger.debug(reqCounter);
		logger.debug(reqHistCounter);
		logger.debug(leaseOwnerMap);
//		logger.debug(leaseMap);
//		logger.info(getTime()+" updateLeases2C ended: ");
		
	}
	
	public void publish(final String msgo, final ITopic<String> outTopic){
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
	public HashSet<Integer> lockAll(ArrayList<Integer> idList) {
		return null;
	}
	@Override
	public void unlockAll(ArrayList<Integer> idList) {
	}
	public enum LeaseType{PERCENTAGE, TWO_CONSECUTIVE};
}
