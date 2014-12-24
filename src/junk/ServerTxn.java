package junk;
import com.hazelcast.config.Config;
import com.hazelcast.config.ManagementCenterConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionOptions.TransactionType;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.*;
import org.json.JSONArray;
import org.json.JSONException;

import seref.CommonTools;
import seref.PAKey;
import seref.PortableVertexFactory;
import seref.Vertex;
import txn.WorkerLockManagerOrdered;

public class ServerTxn<T> {

	private static HazelcastInstance instance;
	private static String name = "-1";
	private static boolean isRandom = true;
	private boolean useCache = true;
	private static int rbctr = 0;
	private static String mes = "";
	private static  CountDownLatch latch1;
	private ArrayList<Integer> nList=null;

	private Integer myID = -1;
	PAKey key, key2;
	private int wNum=-1;
	int p, nodeCount, partitionCount;

	int NLSIZE=7;
	double HIST_PROB=0.8;

	LockManager lm;
	private Logger logger = Logger.getLogger(ServerTxn.class);

	private static volatile int cacheCount=0;
	private static volatile int nocacheCount=0;

	private static long sTime = System.nanoTime();
	public double getTime(){
		return (System.nanoTime()-sTime)/1000000.0;
	}

	public void initInstance(){
		SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
		String curTime = sdf.format(Calendar.getInstance().getTime());

		SimpleLayout layout = new SimpleLayout();    
		FileAppender appender=null;
		try {
			appender = new FileAppender(layout,"worker"+name+"_"+curTime+".log",false);
		} catch (IOException e) {
			e.printStackTrace();
		}    
		logger.addAppender(appender);
		logger.setLevel((Level) Level.INFO);

		logger.debug("Instance initialization started");
		Config cfg = new ClasspathXmlConfig("hazelcast.xml");
		ManagementCenterConfig managementCenterConfig = new ManagementCenterConfig();
		managementCenterConfig.setEnabled(true);
		managementCenterConfig.setUrl("http://localhost:8080/mancenter");
		cfg.setManagementCenterConfig(managementCenterConfig);
		cfg.getGroupConfig().setName("3.0");

		cfg.setInstanceName(name);
		cfg.getSerializationConfig().addPortableFactory(1, new PortableVertexFactory());

		MapConfig mapCfg = new MapConfig();
		mapCfg.setName("vertex");
		mapCfg.setBackupCount(0);
		cfg.addMapConfig(mapCfg);

		QueueConfig qCfg = new QueueConfig();
		qCfg.setName("queue");
		qCfg.setBackupCount(0);
		cfg.addQueueConfig(qCfg);

		System.setProperty("hazelcast.partition.count", wNum+"");

		instance = Hazelcast.newHazelcastInstance(cfg);

		IAtomicLong idGenerator = instance.getAtomicLong("worker-ids");
		myID = (int) idGenerator.getAndIncrement();
		myID= Integer.parseInt(name);

	}

	public void waitMessage(String message, int limit){
		mes = message;
		System.out.println("Waiting for synchronization..");
		ITopic<String> topic1 = instance.getTopic("syncher");
		latch1 = new CountDownLatch(limit);
		topic1.addMessageListener(new MessageListener<String>() {
			public void onMessage(Message<String> msg) {
				logger.debug("Message is: "+msg.getMessageObject());
				if(msg.getMessageObject().equals(mes)){
					latch1.countDown();
					System.out.println("Latch: "+latch1.getCount());
				}
			}
		});
		try {
			latch1.await();
		} catch (InterruptedException e) {e.printStackTrace();}
		logger.debug("Graph loaded.");
	}

	public void readInput(ServerTxn<PAKey> clientP, String path){
		File folder = new File(path);
		File[] listOfFiles = folder.listFiles();

		IMap<PAKey, Vertex> vertexMap;// = context.getMap("vertex");
		IQueue<PAKey> queue;// = context.getQueue("queue");
		IMap<PAKey, Integer> queueMap = null;
		int vertexId = -1;
		int partitionID = -1;

		for (int i = 0; i < listOfFiles.length; i++) {
			File file = listOfFiles[i];
			try {
				BufferedReader br = new BufferedReader(new FileReader(file));
				String line;
				while ((line = br.readLine()) != null) {
					try {
						JSONArray jsonVertex = new JSONArray(line);
						Vertex vertex = new Vertex();
						vertex.setVertexID(jsonVertex.getInt(0));
						vertex.setValue(jsonVertex.getDouble(1));

						vertexId = vertex.getVertexID();

						JSONArray jsonEdgeArray;
						partitionID = jsonVertex.getInt(2) % clientP.wNum;
						vertex.setPartitionID(partitionID);
						jsonEdgeArray = jsonVertex.getJSONArray(3);

						for (int j = 0; j < jsonEdgeArray.length(); ++j) {
							JSONArray jsonEdge = jsonEdgeArray.getJSONArray(j);
							vertex.getOutNeighbors().add(new Integer(jsonEdge.getInt(0)));
						}
						Collections.sort(vertex.getOutNeighbors());

						if(!isRandom){ 
							queue = instance.getQueue("queue@"+partitionID);
						}else{
							int pp = (new Random()).nextInt(wNum);
							queue = instance.getQueue("queue@"+pp);
						}
						queueMap = instance.getMap("queueMap");
						vertexMap = instance.getMap("vertex");

						PAKey t = new PAKey(vertexId, partitionID+"");
						//							logger.debug("Inserted vertex: "+t);
						vertexMap.put(t, vertex);
						IMap<Integer, PAKey> partitionMap = instance.getMap("partitionMap");
						partitionMap.put(vertexId, t);

						Integer id = vertex.getVertexID();
						queue.offer(t);
						queueMap.put(t, id);

					} catch (JSONException e) {
						throw new IllegalArgumentException(
								"next: Couldn't get vertex from line " + line, e);
					}
				}
				br.close(); 
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public int findP(int ID, int nodeCount, int partitionCount){
		double sqrtPC=(Math.floor(Math.sqrt(partitionCount)));
		double pn = Math.floor(Math.sqrt(nodeCount)/sqrtPC);
		int column = (int) (ID%Math.floor(Math.sqrt(nodeCount)));
		int row = (int) (Math.floor(ID/Math.sqrt(nodeCount)));
		int p = (int) (Math.floor(row/pn)*sqrtPC + Math.floor(column/pn));
		return p % wNum;
	}

	boolean sample = false;
	public int compute(TransactionContext context, TransactionOptions options, 
			Vertex vertex, IAtomicLong opCtr, boolean isLazy){

		TransactionalMap<PAKey, Vertex> vertexMap = null;
		context = instance.newTransactionContext(options);
		context.beginTransaction();
		vertexMap = context.getMap("vertex");
		opCtr.incrementAndGet();

		HashSet<Integer> leaseMap;

		try {    
			//			nList = NeighborSelector.selectWeighted(nodeCount, wNum, myID, NLSIZE);
			nList = NeighborSelector.selectHistorical(nodeCount, wNum, myID, NLSIZE, nList, HIST_PROB);
			logger.debug("nList is: "+nList);

			leaseMap = lm.lockAll(nList);

			if(isLazy && useCache){
				synchronized(lm.lazyCache){
//					HashSet<Integer> idSet = new HashSet<Integer>(nList);
//					for(Integer vid : lm.lazyCache.keySet()){
//						if(!idSet.contains(vid)){
//							p = findP(vid, nodeCount, partitionCount);
//							key = new PAKey(vid, p+"");
//							vertexMap.put(key, lm.lazyCache.get(vid));
//							logger.info(getTime()+" Lazy vertex is written: "+vid);
//						}
//					}
					lm.prepareNewLazyCache(nList);
				}
			}

			for(int i=0; i<NLSIZE; i++){
				int nID = nList.get(i);
				//				logger.debug("Reading "+neighbor);
				p = findP(nID, nodeCount, partitionCount);
				key = new PAKey(nID, p+"");
				if(!sample){
					sample=true;
					lm.sampleVertex=vertexMap.get(key);
				}
				Vertex nVertex=null;
				boolean found = false;
				if(useCache){
					if(isLazy){
						synchronized(lm.lazyCache){
							if(lm.newLazyCache.get(nID)!=null){
								nVertex = lm.newLazyCache.get(nID);
//								lm.lazyCache.put(nID, nVertex);
								logger.info(getTime()+" Read from lazy cache "+nID);
								cacheCount++;
								found = true;
							}
						}
					}
					if(!found){
						if(leaseMap.contains(nID)){
							if(lm.vertexCache.get(nID)==null){
								nVertex = vertexMap.get(key);
								lm.vertexCache.put(nID, nVertex);
								logger.info(getTime()+" Read normal-write to cache "+nID);
								cacheCount++;
							}else{
								nVertex = lm.vertexCache.get(nID);
								logger.info(getTime()+" Read from vertex cache "+nID);
								cacheCount++;
							}
						}else{
							nVertex = vertexMap.get(key);
							if(!isLazy) vertexMap.put(key, nVertex);
							logger.info(getTime()+" Read and wrote normally(!lazy) "+nID);
							nocacheCount++;
						}
					}
				}
				else{
					nVertex = vertexMap.get(key);
					vertexMap.put(key, nVertex);
					logger.info(getTime()+" Read and wrote normally "+nID);
					nocacheCount++;
				}
				if(nVertex==null) nVertex = vertexMap.get(key);

				if(useCache && isLazy){ 
					synchronized(lm.lazyCache){
						lm.lazyCache.put(nID, nVertex);
					}				
				}
			}

			context.commitTransaction();
			logger.info(getTime()+" Committed "+nList);

			lm.unlockAll(nList);

		}catch (Throwable t)  {
			lm.unlockAll(nList);
			t.printStackTrace();
			//			logger.debug("Rollback "+vertex.getVertexID()+" with error "+t);
			rbctr++;
			context.rollbackTransaction();
		}
		return 0;
	}

	public static void main(String[] args) {    

		ServerTxn<PAKey> clientP = new ServerTxn<PAKey>();

		if(args.length>1) clientP.wNum = Integer.parseInt(args[1]);
		int status=0;
		name=args[0];
		int TOTAL_OPS = Integer.parseInt(args[2]);
		clientP.NLSIZE = Integer.parseInt(args[3]);
		clientP.HIST_PROB = Double.parseDouble(args[4]);
		boolean isLazy = false;
		if(args.length>5){
			if(args[5].equals("lazy")){ isLazy=true;}
			else if(args[5].equals("nolazy")){ isLazy=false;}
		}
		if(args.length>6){
			if(args[6].equals("cache")) clientP.useCache = true;
			if(args[6].equals("nocache")) clientP.useCache = false;
		}	
		isRandom = false;

		clientP.initInstance();

		TransactionOptions options = new TransactionOptions().setTransactionType(
				TransactionType.TWO_PHASE);
		options.setTimeout(clientP.NLSIZE*10+10000, TimeUnit.MILLISECONDS);
		TransactionContext context = null;//instance.newTransactionContext(options);
		IMap<PAKey, Vertex> vertexMapI = null;

		//		Cluster cluster = instance.getCluster();
		//		IMap<Integer, InetSocketAddress> memberMap = instance.getMap("memberMap");
		//		memberMap.put(clientP.myID, cluster.getLocalMember().getInetSocketAddress());

		clientP.waitMessage("Loaded",1);

		/*
				InetSocketAddress clientAddr = (InetSocketAddress) instance.getClientService().getConnectedClients().iterator().next().getSocketAddress();
				memberMap.put(-1, clientAddr);

				Kryo kryo = new Kryo(); // version 2.x
				for(Entry<Integer, InetSocketAddress> entry : memberMap.entrySet()){
					System.out.println(entry.getKey()+" "+entry.getValue());
				}
				try {
					System.out.println(clientP.getTime()+" Start socket 1");
					Socket requestSocket = new Socket(memberMap.get(0).getAddress(), memberMap.get(0).getPort());
					ObjectOutputStream out = new ObjectOutputStream(requestSocket.getOutputStream());
					System.out.println(clientP.getTime()+" Start socket 2");
					Output output = new Output(out);
					System.out.println(clientP.getTime()+" Start socket 3");
					kryo.writeObject(output, "Hello");
					System.out.println(clientP.getTime()+" Start socket 4");
					out.writeObject("HELLO");
					System.out.println(clientP.getTime()+" Start socket 5");
					out.flush();
					System.out.println(clientP.getTime()+" Start socket 6");
					out.close();
					requestSocket.close();
					System.out.println(clientP.getTime()+" End socket ");
				} catch (IOException e) {
					e.printStackTrace();
				}
				CommonTools.getInput();
		 */

		IAtomicLong opCtr = instance.getAtomicLong("operationCounter");
		vertexMapI = instance.getMap("vertex");
		clientP.nodeCount = vertexMapI.size();
		clientP.logger.debug("vertexMapI size is:"+clientP.nodeCount);

		boolean isBatch = true;
		if(isBatch){
			clientP.lm = new WorkerLockManagerOrdered(instance, clientP.myID, clientP.logger, isLazy, sTime);
		}else{
			clientP.lm = new WorkerLockManager(instance, clientP.myID, clientP.logger, isLazy, sTime);
		}

		clientP.logger.info(args);
		clientP.logger.info("Local vertex list is: "+vertexMapI.localKeySet());
		CommonTools.sleep(500);

		clientP.partitionCount = clientP.nodeCount <= 64 ? 4 : 16;

		long startTime = System.nanoTime();    
		boolean endOfWarmup = false;

		int opCounter = 0;
		if (clientP.myID>-1){
			while(opCtr.get()<TOTAL_OPS){
				Vertex vertex = null;
				double computeStartTime = System.nanoTime();
				status = clientP.compute(context, options, vertex, opCtr, isLazy);
				double computeElapsedTime = (System.nanoTime() - computeStartTime)/(Math.pow(10, 6));
				clientP.logger.info(opCtr.get()+" Compute time is:"+computeElapsedTime);

				long globalOps = opCtr.get();
				if(globalOps/100 != opCounter || status == -1){
					opCounter = (int) (globalOps/100);
					if((int) (globalOps/100) == 2 && !endOfWarmup){
						endOfWarmup = true;
						double elapsedTime = (System.nanoTime() - startTime)/(Math.pow(10, 9));
						System.out.println(globalOps+" Warmup time is:"+elapsedTime);
						startTime = System.nanoTime();
					}else{
						opCounter = (int) (globalOps/100);
						double elapsedTime = (System.nanoTime() - startTime)/(Math.pow(10, 9));
						clientP.logger.info(globalOps+" Elapsed time is:"+elapsedTime);
						System.out.println(globalOps+" Elapsed time is:"+elapsedTime);
					}
				}
				if(status==-1){
					break;
				}
			}
		}
		double elapsedTime = (System.nanoTime() - startTime)/(Math.pow(10, 9));
		clientP.logger.info(opCtr.get()+" Total time is:"+elapsedTime);
		System.out.println(opCtr.get()+" Total time is:"+elapsedTime);
		clientP.logger.info("Rollback count is:"+rbctr);
		clientP.logger.info(" Cache rate is: "+cacheCount+"/"+nocacheCount);
		System.out.println(" Cache rate is: "+cacheCount+"/"+nocacheCount);


		IAtomicLong finishedCtr = instance.getAtomicLong("finishedCtr");
		ITopic<String> topic1 = instance.getTopic("syncher");
		long t = finishedCtr.incrementAndGet();
		if(t!=clientP.wNum+1){
			clientP.waitMessage("Finished",(int) (clientP.wNum+1-t));
		}
		topic1.publish("Finished");

		instance.getLifecycleService().shutdown();
	}
}
