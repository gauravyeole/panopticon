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
import java.util.ListIterator;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import seref.PAKey;
import seref.Vertex;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;

import org.jeromq.ZMQ;
import org.jeromq.ZMQ.Context;
import org.jeromq.ZMQ.Socket;

/**
 * @author aeyate
 *
 */
public class WorkerLockManagerStagedJeroMQ extends LockManager{
	private HashSet<Integer> requestedMap = new HashSet<Integer>();
	private HashSet<Integer> lockMap = new HashSet<Integer>();
	private HashSet<Integer> isLockedMap = new HashSet<Integer>();
	private HashSet<Integer> leaseMap = new HashSet<Integer>();
	private HashSet<Integer> lazyList = new HashSet<Integer>();
	
	private HashSet<Integer> mlSet = null;


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
	private IMap<PAKey, Vertex> vertexMapI;
	private ParamHolder ph;

	private LinkedBlockingQueue<String> lbq = new LinkedBlockingQueue<String>();
    private static ExecutorService executors = Executors.newFixedThreadPool(1);
    
    Context pcontext = ZMQ.context(1);
    Context scontext = ZMQ.context(1);
    Socket publisher = pcontext.socket(ZMQ.PUSH);
    Socket subscriber = scontext.socket(ZMQ.SUB);
    String masterAddr = null;
    private IMap<String, String> ipMap; 

	public WorkerLockManagerStagedJeroMQ(HazelcastInstance instance, Integer myID, Logger logger, boolean isLazy, long sTime, ParamHolder ph) {
		vertexMapI = instance.getMap("vertex");
		ipMap = instance.getMap("ipMap");
		this.startTime = sTime;
		this.workerID = myID;
		this.logger = logger;
		this.isLazy = isLazy;
		this.ph = ph;
		outTopic = instance.getTopic("toMaster");
		inTopic = instance.getTopic("fromMasterTo"+myID);
		
		ipMap = instance.getMap("ipMap");
//		String myIP = "localhost";
		String myIP = instance.getCluster().getLocalMember().getInetSocketAddress().getAddress().toString();
		int myPort = instance.getConfig().getNetworkConfig().getPort();
		int jmqPort = myPort+100+myID;
		ipMap.put(myID+"", "tcp:/"+myIP+":"+jmqPort); // TODO: Bu degerlerin dogru oldugundan emin ol
		logger.info("My ip is: "+ipMap.get(myID+""));
		System.out.println("My ip is: "+"tcp:/"+myIP+":"+jmqPort);
		System.out.println("My ip is: "+ipMap.get(myID+""));
		subscriber.bind(ipMap.get(myID+"")); // TODO: Burda port'u ne secmemiz gerekiyor?
        subscriber.subscribe("fromMasterTo"+myID);
        
        new Thread( new Runnable(){
        	public void run(){
        		listenRequests();
        	}
        }).start();
		
	}

	public HashSet<Integer> lockAll(ArrayList<Integer> idList){

		txnCache.clear();
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
			if(iHaveAll){
				txnActive = true;
				localTxnCtr++;
			}
		}

		String mess = reqList.toString();
//		if(!iHaveAll)
		{
//			logger.info(getTime() + ": Sending message: "+ mess);
//			(new SntpClient()).printNTPTime(logger);

//			publish(mess,outTopic);
			publishZMQ(mess);
		}
//		logger.info(getTime() + ": Message sent: "+ mess);

		while(!checkIfIHaveAllFast(idList)){
//			logger.info(getTime() + ": Sending message: "+ mess);
//			outTopic.publish(mess);
			logger.debug(getTime()+" Waiting for reply for txn "+txnID);		
			waitLocker(0);
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

	private boolean checkIfIHaveAllFast(ArrayList<Integer> idList) {
		boolean iHaveAll = true;
		ListIterator<Integer> it = idList.listIterator();
		synchronized(lockMap){
			while(it.hasNext()){
				Integer vertexID = it.next();
				if(!lockMap.contains(vertexID)){
					logger.debug(getTime()+" I do not have item "+vertexID);
					iHaveAll = false;
				}else{
					int p = ServerTxn.findP(vertexID, ph.nodeCount, ph.partitionCount);
					PAKey key = new PAKey(new Integer(vertexID), p+"");
					if(!txnCache.containsKey(vertexID)){
						if(lazyCache.containsKey(vertexID)){
							txnCache.put(vertexID, lazyCache.get(vertexID));
						}else if(vertexCache.containsKey(vertexID)){
							txnCache.put(vertexID, vertexCache.get(vertexID));
						}else{
							logger.info(getTime()+" Adding to txnCache "+vertexID);
							txnCache.put(vertexID, vertexMapI.get(key));
						}
					}
				}
			}
			logger.info(getTime()+" txnCache: "+txnCache.keySet());
		}
		return iHaveAll;
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
//			publish(mess,outTopic);
			publishZMQ(mess);
		}	
	}

	private void waitLocker(int timeMillis){
		try {
			synchronized(locker){
				if(noReply)	locker.wait(timeMillis);
				noReply = true;
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void listenRequests(){
//		inTopic.addMessageListener(new MessageListener<String>() {
//			public void onMessage(Message<String> msg) {
		while(true){
				String msg = subscriber.recvStr();
				if(msg.startsWith("from")) continue;
				mctr++;
//				(new SntpClient()).printNTPTime(logger);
				synchronized(lockMap){logger.debug("lockMap: "+lockMap);}
				logger.info(getTime() + ": Message received: "+msg);
				HashSet<String> msgSetStr;
				HashSet<Integer> msgSet;

				if(msg.startsWith("request_")){
					String[] parts = msg.split("_");
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

				}else if(msg.startsWith("reply") 
						|| msg.startsWith("ml")){
					String[] parts = msg.split("_");
					msgSetStr=new HashSet<String>(Arrays.asList(parts).subList(3, parts.length));
					
					msgSet = convertToInteger(msgSetStr);
					checkifRequested(msgSetStr);
					if(msg.startsWith("ml")){
//						if(!useML) return;
						mlSet = msgSet;
					}
					addLocks(msgSet);
//					if(checkIfIHaveAll(idList))
					{
						txnActive = true;
					}
					synchronized(locker){
						noReply = false;
						locker.notify();
					}	

				}else if(msg.startsWith("lease_")){
					String[] parts = msg.split("_");
					msgSetStr=new HashSet<String>(Arrays.asList(parts).subList(2, parts.length));
					msgSet = convertToInteger(msgSetStr);
					updateLeases(msgSet,false);
				}

				logger.debug(getTime()+" Finished onMessage");
				synchronized(lockMap){logger.debug("lockMap: "+lockMap);}
			}
//		});
	}

	/**
	 * Adds the msgSet to lockMap. Nothing else.
	 */
	protected void addLocks(HashSet<Integer> msgSet) {
		synchronized(lockMap){
			for(Integer vid:msgSet){
				lockMap.add(vid);
			}
		}		
	}
	
	/**
	 * If an item in msgSet starts with '+', then adds this item to requestedMap
	 */
	protected void checkifRequested(HashSet<String> msgSet) {
		synchronized(lockMap){
			for(String vid:msgSet){
				if(vid.charAt(0)=='+'){
					requestedMap.add(Integer.parseInt(vid));
				}
			}
		}		
	}

	/**
	 * Adds all items in msgSet to requestedMap
	 */
	protected void addToRequestedMap(HashSet<Integer> msgSet) {
		synchronized(lockMap){
			for(Integer vid:msgSet){
				requestedMap.add(vid);
			}
		}
		synchronized(lockMap){logger.debug("requestedMap: "+requestedMap);}
	}

	/**
	 * If any of the items in msgSet are in idSet(i.e. nlist), returns false. Else returns true
	 */
	protected boolean areAllAvailable(HashSet<Integer> msgSet) {
		for(Integer vid:msgSet){
			if(idSet.contains(vid)){
				return false;
			}
		}
		return true;
	}

	/**
	 * For all items in msgSet, if they are in lockMap, removes them from lockMap, lazyList and lazyCache
	 * and then publishes those items to broker.
	 */
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

//			publish(mess,outTopic);
			publishZMQ(mess);
		}
	}

	/**
	 * @param msgSet
	 * @param isRemove
	 * If isRemove true, remove all items in msgSet from leaseMap and vertexCache.
	 * 		Also if lazyCache or leaseMap contains an item, add dummy item to IMap. <br>
	 * If isRemove false, just add all items to leaseMap
	 */
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
	
	/**
	 * 
	 */
	public void unlockAll(ArrayList<Integer> idList){
		
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
			if(!isLazy && useML){ // this block was added for ML. Return incorrectly predicted and received locks
				Iterator<Integer> it = lockMap.iterator();
				while(it.hasNext()){
					Integer vid = it.next();
					if(!leaseMap.contains(vid) && mlSet != null && !mlSet.contains(vid)){
						isEmpty = false;
						repList.append("_").append(vid);
						it.remove();
					}
				}
			}
			requestedMap.clear();
			idSet.clear();
		}
		txnActive = false;
		String mess = repList.toString();
		if(!isEmpty){
//			publish(mess,outTopic);
			publishZMQ(mess);				
		}
		logger.debug(getTime()+" Finished unlockAll");
		synchronized(lockMap){logger.debug("lockMap: "+lockMap);}
	}

	public void publishZMQ(final String msgo){
		logger.info(getTime() + ": Sending message: "+ msgo);
		if(masterAddr==null){
			masterAddr = ipMap.get("-1");
			System.out.println(masterAddr);
			publisher.connect(masterAddr);
		}
		publisher.sendMore("toMaster");
        publisher.send(msgo);
	}
	
	public void publish(final String msgo, final ITopic<String> outTopic){
		logger.info(getTime() + ": Sending message: "+ msgo);
		try {
			lbq.put(msgo);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		executors.execute(new Runnable() {
			public synchronized void run() {
				while(!lbq.isEmpty()){
					String mess=lbq.poll();
					outTopic.publish(mess);
				}				
			}
		});
//		logger.info(getTime() + ": Message sent: "+ msgo);

	}


	@Override
	public void cleanup() {}
}
