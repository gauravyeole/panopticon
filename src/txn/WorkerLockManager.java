/**
 * 
 */
package txn;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.ListIterator;

import org.apache.log4j.Logger;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;

/**
 * @author aeyate
 *
 */
public class WorkerLockManager extends LockManager{
	private HashSet<Integer> requestedMap = new HashSet<Integer>();
	private HashSet<Integer> lockMap = new HashSet<Integer>();
	private HashSet<Integer> isLockedMap = new HashSet<Integer>();
	private HashSet<Integer> leaseMap = new HashSet<Integer>();
	private HashSet<Integer> lazyList = new HashSet<Integer>();

	private ITopic<String> inTopic, outTopic;
	private Integer workerID;
	private static volatile boolean anyNewReply = false;
	private static volatile Integer msgCtr = 0;
	
	private Object locker = new Object();
	
	private boolean isLazy = false;

	private int mctr=0;

	private Logger logger;

	public WorkerLockManager(HazelcastInstance instance, Integer myID, Logger logger, boolean isLazy, long sTime) {
		this.startTime = sTime;
		this.workerID = myID;
		this.logger = logger;
		this.isLazy = isLazy;
		outTopic = instance.getTopic("toMaster");
		inTopic = instance.getTopic("fromMasterTo"+myID);
		listenRequests();
	}

	public HashSet<Integer> lockAll(ArrayList<Integer> idList){
		logger.info(getTime()+" Started LockAll");
//		logger.debug("requestedMap: "+requestedMap);
		synchronized(lockMap){logger.debug("isLockedMap: "+isLockedMap);}
		synchronized(lockMap){logger.debug("lockMap: "+lockMap);}
		
		Collections.sort(idList);
		logger.info(getTime() + ": I need : "+ idList);
		if(isLazy){
			flushLazyList(idList);
		}
		String reqList = "";
		boolean token = true;
		int firstUnownedIndex = -1;
		ListIterator<Integer> it = idList.listIterator();
		synchronized(lockMap){
			while(it.hasNext()){
				Integer vertexID = it.next();
				if(lockMap.contains(vertexID)){
					if(!isLockedMap.contains(vertexID) && token){
						isLockedMap.add(vertexID);
					}
				}else{
					reqList+="_"+vertexID;
					if(token){
						firstUnownedIndex = it.previousIndex();
					}
					token = false;
				}
			}
		}
		String mess = "request_"+(++msgCtr)+"_"+workerID+reqList;
		if(!reqList.equals("")){
			logger.info(getTime() + ": Sending message: "+ mess);
			outTopic.publish(mess);
		}else{
			firstUnownedIndex = it.previousIndex();
		}
		
		it = idList.listIterator(firstUnownedIndex);
		while(it.hasNext()){
			Integer vertexID = it.next();

			boolean b;
			synchronized(lockMap){b = lockMap.contains(vertexID);}
			if(b){
				synchronized(lockMap){
					if(lockMap.contains(vertexID) && !isLockedMap.contains(vertexID)){
						isLockedMap.add(vertexID);
					}
				}
			}
			else{
				logger.debug("Waiting for "+vertexID);
				mess = "rerequest_"+(msgCtr)+"_"+workerID+"_"+vertexID;
				logger.info(getTime() + ": Resending request message: "+ mess);
				outTopic.publish(mess);
				if(it.hasPrevious()) it.previous();
				if(!anyNewReply){
					synchronized(locker){
						long timeoutExpiredMs = System.currentTimeMillis() + 5;
						while(!lockMap.contains(vertexID)){
							waitLocker();
							if (System.currentTimeMillis() >= timeoutExpiredMs) {
							    break;
							}
						}
					}
					anyNewReply = false;
				}
				else{
					anyNewReply = false;
				}
				logger.debug("Checking locks again..");
			}
		}
//		logger("requestedMap: "+requestedMap);
		synchronized(lockMap){logger.debug("isLockedMap: "+isLockedMap);}
		synchronized(lockMap){logger.debug("lockMap: "+lockMap);}
		logger.info(getTime()+" Finished LockAll");
		return leaseMap;
	}
	
	private void flushLazyList(ArrayList<Integer> idList) {
		for(Integer vid : idList){
			lazyList.remove(vid);
		}
		String reqList = "";
		for(Integer vid : lazyList){
			lockMap.remove(vid);
			reqList+="_"+vid;
		}
		String mess = "reply_"+(++msgCtr)+"_"+workerID+reqList;
		if(!reqList.equals("")){
			logger.info(getTime() + ": Flushing lazy list: "+ mess);
			outTopic.publish(mess);
		}	
	}

	private void waitLocker(){
		try {
			locker.wait(100);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
		
//	private double getTime(){
//		return (System.nanoTime()-startTime)/1000000.0;
//	}

	public void listenRequests(){
		inTopic.addMessageListener(new MessageListener<String>() {
			public void onMessage(Message<String> msg) {
				mctr++;
				logger.info(getTime() + ": Message received: "+msg.getMessageObject());
//				logger("requestedMap: "+requestedMap);
				synchronized(lockMap){logger.debug("isLockedMap: "+isLockedMap);}
				synchronized(lockMap){logger.debug("lockMap: "+lockMap);}
				if(msg.getMessageObject().startsWith("request_")){
					String[] parts = msg.getMessageObject().split("_");
					String reqList = "";
					for(int i=3; i<parts.length; i++){
						Integer vid = Integer.parseInt(parts[i]);
						synchronized(lockMap){
							if(lockMap.contains(vid)){
								if(!isLockedMap.contains(vid)){
									lockMap.remove(vid);
									reqList+="_"+vid;
									leaseMap.remove(vid);
									if(isLazy){
										lazyList.remove(vid);
									}
								}else{
									requestedMap.add(vid);
								}
							}
						}
					}
					if(!reqList.equals("")){
						String mess = "reply_"+(++msgCtr)+"_"+workerID+reqList;
						logger.info(getTime() + ": Sending message: "+ mess);
						outTopic.publish(mess);
					}
				}
				else if(msg.getMessageObject().startsWith("reply_")){
					String[] parts = msg.getMessageObject().split("_");
					for(int i=3; i<parts.length; i++){
						anyNewReply = true;
						Integer vid = Integer.parseInt(parts[i]);
						
						synchronized(lockMap){
							lockMap.add(vid);
						}
						synchronized(locker){
							locker.notify();
						}
					}
				}
				else if(msg.getMessageObject().startsWith("lease_")){
					String[] parts = msg.getMessageObject().split("_");
					Integer vid = Integer.parseInt(parts[2]);
					synchronized(lockMap){
						if(parts[1].equals("add")){
							leaseMap.add(vid);
						}else if(parts[1].equals("remove")){
							leaseMap.remove(vid);
						}
					}
				}
//				logger("requestedMap: "+requestedMap);
				synchronized(lockMap){logger.debug("isLockedMap: "+isLockedMap);}
				synchronized(lockMap){logger.debug("lockMap: "+lockMap);}
				logger.debug("Finished onMessage");
			}
		});
	}

	public void unlockAll(ArrayList<Integer> idList){
		Collections.sort(idList);
		logger.debug("Started unlockAll of: "+ idList);
//		logger.debug("requestedMap: "+requestedMap);
		synchronized(lockMap){logger.debug("isLockedMap: "+isLockedMap);}
		synchronized(lockMap){logger.debug("lockMap: "+lockMap);}
		synchronized(lockMap){logger.debug("leaseMap: "+leaseMap);}
		String reqList = "";
		for(Integer vertexID:idList){		
			synchronized(lockMap){
				isLockedMap.remove(vertexID);
				if(requestedMap.contains(vertexID))
				{
					leaseMap.remove(vertexID);
					lockMap.remove(vertexID);
					reqList+="_"+vertexID;
				}else if( !leaseMap.contains(vertexID)){
					if(isLazy){
						lazyList.add(vertexID);
					}else{
						lockMap.remove(vertexID);
						reqList+="_"+vertexID;
					}
				}
				requestedMap.remove(vertexID);
			}
		}
		String mess = "reply_"+(++msgCtr)+"_"+workerID+reqList;
		if(!reqList.equals("")){
			logger.info(getTime() + ": Sending message: "+ mess);
			outTopic.publish(mess);
		}
//		logger.debug("requestedMap: "+requestedMap);
		synchronized(lockMap){logger.debug("isLockedMap: "+isLockedMap);}
		synchronized(lockMap){logger.debug("lockMap: "+lockMap);}
		synchronized(lockMap){logger.debug("Finished unlockAll");}
	}

	@Override
	public void cleanup() {
	}
}
