/**
 * 
 */
package txn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.ListIterator;
import java.util.Random;

import org.apache.log4j.Logger;

import seref.PAKey;
import seref.Vertex;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;

/**
 * @author aeyate
 *
 */
public class WorkerLockManagerOrdered extends LockManager{
	private HashSet<Integer> requestedMap = new HashSet<Integer>();
	private HashSet<Integer> lockMap = new HashSet<Integer>();
	private HashSet<Integer> isLockedMap = new HashSet<Integer>();
	private HashSet<Integer> leaseMap = new HashSet<Integer>();
	private HashSet<Integer> lazyList = new HashSet<Integer>();

	private HashSet<Integer> idSet;
	private ArrayList<Integer> idList;

	private ITopic<String> inTopic, outTopic;
	private Integer workerID;
	private volatile Integer msgCtr = 0;
	private volatile Integer txnID = 0;
	private volatile boolean txnActive = false;
	private volatile boolean noReply = true;

	private Object locker = new Object();

	private boolean isLazy = false;
	private boolean localTxn = false;

	private int mctr=0;

	private Logger logger;
	private IMap<Object, Object> vertexMapI;

	public WorkerLockManagerOrdered(HazelcastInstance instance, Integer myID, Logger logger, boolean isLazy, long sTime) {
		vertexMapI = instance.getMap("vertex");
		this.startTime = sTime;
		this.workerID = myID;
		this.logger = logger;
		this.isLazy = isLazy;
		outTopic = instance.getTopic("toMaster");
		inTopic = instance.getTopic("fromMasterTo"+myID);
		listenRequests();
	}

	public HashSet<Integer> lockAll(ArrayList<Integer> idList){
		/*ALGORITHM:
		 * flushlazyList(idList);
		 * if(localTxn) mark idList busy and return;
		 * String txnList = convertToString(idlist);
		 * sendMsg(beginTxn_wID_(txnID++)_txnList);
		 * waitReply();
		 * if(IHaveAllLocks(idlist)) return;
		 * else error("Missing locks"); 
		 */

		txnID++;
		localTxn = false;
		idSet = new HashSet<Integer>(idList);
		this.idList = idList;
		logger.info(getTime()+" Started LockAll");
		//		logger.debug("requestedMap: "+requestedMap);
		synchronized(lockMap){logger.debug("isLockedMap: "+isLockedMap);}
		synchronized(lockMap){logger.debug("lockMap: "+lockMap);}
		synchronized(lockMap){logger.info("leaseMap: "+leaseMap);}


		Collections.sort(idList);
		logger.info(getTime() + ": I need : "+ idList);

		if(isLazy){
			flushLazyList(idList);
		}

		//String.format("%05d", vid)
		StringBuilder reqList = new StringBuilder("beginTxn_"+txnID+"_"+workerID);
		boolean iHaveAll = true;
		ListIterator<Integer> it = idList.listIterator();
		synchronized(lockMap){
			while(it.hasNext()){
				Integer vid = it.next();
				reqList.append("_").append(vid);
				if(iHaveAll && !lockMap.contains(vid)){
					iHaveAll = false;
				}
			}
		}

		String mess = reqList.toString();
		if(iHaveAll){
			localTxn = true;
		}
		else{
			logger.info(getTime() + ": Sending message: "+ mess);
//			(new SntpClient()).printNTPTime(logger);
			outTopic.publish(mess);
		}


		while(!checkIfIHaveAll(idList)){
//			logger.info(getTime() + ": Sending message: "+ mess);
//			outTopic.publish(mess);
			logger.debug(getTime()+" Waiting for reply for txn "+txnID);		
			if(noReply) waitLocker(0);
			noReply = true;
		}


		synchronized(lockMap){logger.debug("lockMap: "+lockMap);}
		logger.info(getTime()+" Finished LockAll");
		return leaseMap;
	}

	private boolean checkIfIHaveAll(ArrayList<Integer> idList) {
		ListIterator<Integer> it = idList.listIterator();
		synchronized(lockMap){
			while(it.hasNext()){
				Integer vertexID = it.next();
				if(!lockMap.contains(vertexID)){
					logger.debug(getTime()+" I do not have all items for txn "+txnID);
					return false;
				}
			}
		}
		return true;
	}

	
	private void flushLazyList(ArrayList<Integer> idList) {
		StringBuilder reqList = new StringBuilder("lazy_"+txnID+"_"+workerID);
		boolean isEmpty = true;

		synchronized(lockMap){
			for(Integer vid : idList){
				lazyList.remove(vid);
			}
			for(Integer vid : lazyList){
				if(!leaseMap.contains(vid)){
					isEmpty = false;
					lockMap.remove(vid);
					reqList.append("_").append(vid);
					if(lazyCache.containsKey(vid) || leaseMap.contains(vid)){
						vertexMapI.put(new PAKey((new Random()).nextInt(100)-200, 
								(new Random()).nextInt(1000)+""), sampleVertex);
					}
				}
			}
		}
//		updateLeases(lazyList,true); // This looks wrong?
		
		String mess = reqList.toString();
		lazyList.clear();
		if(!isEmpty){
			logger.info(getTime() + ": Flushing lazy list: "+ mess);
			outTopic.publish(mess);
		}	
	}

	private void waitLocker(int timeMillis){
		try {
			synchronized(locker){
				locker.wait(timeMillis);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void listenRequests(){
		inTopic.addMessageListener(new MessageListener<String>() {
			public void onMessage(Message<String> msg) {
				mctr++;
//				(new SntpClient()).printNTPTime(logger);
				synchronized(lockMap){logger.debug("lockMap: "+lockMap);}
				logger.info(getTime() + ": Message received: "+msg.getMessageObject());
				HashSet<String> msgSetStr;
				HashSet<Integer> msgSet;

				if(msg.getMessageObject().startsWith("request_")){
					String[] parts = msg.getMessageObject().split("_");
					msgSetStr=new HashSet<String>(Arrays.asList(parts).subList(3, parts.length));
					msgSet = convertToInteger(msgSetStr);
					if(!txnActive){
						updateLeases(msgSet, true);
						giveLocks(msgSet);
					}else if(areAllAvailable(msgSet)){
						updateLeases(msgSet, true);
						giveLocks(msgSet);
					}else{
						//						updateLeases(msgSet, true);
						addToRequestedMap(msgSet);
					}

				}else if(msg.getMessageObject().startsWith("reply_")){
					String[] parts = msg.getMessageObject().split("_");
					msgSetStr=new HashSet<String>(Arrays.asList(parts).subList(3, parts.length));
					msgSet = convertToInteger(msgSetStr);
					addLocks(msgSet);
//					if(checkIfIHaveAll(idList))
					{
						txnActive = true;
						noReply = false;
					}
					synchronized(locker){locker.notify();}	

				}else if(msg.getMessageObject().startsWith("lease_")){
					String[] parts = msg.getMessageObject().split("_");
					msgSetStr=new HashSet<String>(Arrays.asList(parts).subList(2, parts.length));
					msgSet = convertToInteger(msgSetStr);
					updateLeases(msgSet,false);
				}

				logger.debug(getTime()+" Finished onMessage");
				synchronized(lockMap){logger.debug("lockMap: "+lockMap);}
			}
		});
	}

	protected void addLocks(HashSet<Integer> msgSet) {
		synchronized(lockMap){
			for(Integer vid:msgSet){
				lockMap.add(vid);
			}
		}		
	}

	protected void addToRequestedMap(HashSet<Integer> msgSet) {
		synchronized(lockMap){
			for(Integer vid:msgSet){
				requestedMap.add(vid);
			}
		}
		synchronized(lockMap){logger.debug("requestedMap: "+requestedMap);}
	}

	protected boolean areAllAvailable(HashSet<Integer> msgSet) {
		for(Integer vid:msgSet){
			if(idSet.contains(vid)){
				return false;
			}
		}
		return true;
	}

	protected void giveLocks(HashSet<Integer> msgSet) {
		StringBuilder repList = new StringBuilder("reply_"+(msgCtr++)+"_"+workerID);
		boolean isEmpty = true;
		synchronized(lockMap){
			for(Integer vid:msgSet){	
				if(lockMap.contains(vid)){
					lazyList.remove(vid);
					synchronized(lazyCache){lazyCache.remove(vid);}
					lockMap.remove(vid);
					repList.append("_").append(vid);
					isEmpty = false;
				}
			}
		}
		if(!isEmpty){
			String mess = repList.toString();
			logger.info(getTime() + ": Sending message: "+ mess);
			outTopic.publish(mess);
		}
	}

	protected void updateLeases(HashSet<Integer> msgSet, boolean isRemove) {
		synchronized(lockMap){
			for(Integer vid:msgSet){
				if(isRemove){
					if(lazyCache.containsKey(vid) || leaseMap.contains(vid)){
						vertexMapI.put(new PAKey((new Random()).nextInt(100)-200, 
								(new Random()).nextInt(1000)+""), sampleVertex);		
					}
					leaseMap.remove(vid);
					vertexCache.remove(vid);
				}else{
					leaseMap.add(vid);
				}
			}
		}
		synchronized(lockMap){logger.debug("leaseMap: "+leaseMap);}
	}

	public void unlockAll(ArrayList<Integer> idList){
		/*ALGORITHM:
		 * if(waiting Req) updateLeases, replyReq;
		 * if(!lazy) send all without lease with msg "endTxn_wid_txnID_repList"
		 */
		synchronized(lockMap){logger.debug("lockMap: "+lockMap);}
		synchronized(lockMap){logger.debug("leaseMap: "+leaseMap);}
		logger.debug("Started unlockAll of: "+ idList);

		StringBuilder repList = new StringBuilder("endTxn_"+txnID+"_"+workerID);
		boolean isEmpty = true;
		synchronized(lockMap){
			if(requestedMap.size()>0) isEmpty = false;
			for(Integer vid:requestedMap){
				lockMap.remove(vid);
				if(lazyCache.containsKey(vid) || leaseMap.contains(vid)){
					vertexMapI.put(new PAKey((new Random()).nextInt(100)-200, 
							(new Random()).nextInt(1000)+""), sampleVertex);		
				}
				leaseMap.remove(vid);
				synchronized(lazyCache){lazyCache.remove(vid);}
				vertexCache.remove(vid);
				repList.append("_").append(vid);				
			}
			for(Integer vid:idList){
				if(requestedMap.contains(vid)){}
				else if( !leaseMap.contains(vid)){
					if(isLazy){
						lazyList.add(vid);
					}else{
						isEmpty = false;
						lockMap.remove(vid);
						repList.append("_").append(vid);
					}
				}
			}
			requestedMap.clear();
			idSet.clear();
		}
		txnActive = false;
		String mess = repList.toString();
		if(!isEmpty){
			logger.info(getTime() + ": Sending message: "+ mess);
			outTopic.publish(mess);
		}
		logger.debug(getTime()+" Finished unlockAll");
		synchronized(lockMap){logger.debug("lockMap: "+lockMap);}


	}


	@Override
	public void cleanup() {}
}
