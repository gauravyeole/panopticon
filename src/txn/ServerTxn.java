package txn;
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
import java.io.FileNotFoundException;
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

public class ServerTxn<T> {

	private static HazelcastInstance instance; 
	private static String wid = "-1";
	private static boolean isRandom = true;
	private boolean useCache = true;
	private static int rbctr = 0;
	private static String mes = "";
	private static  CountDownLatch latch1;
	private ArrayList<Integer> nList=null;

	private Integer myID = -1;
	PAKey key, key2;
	private static int numWorkers=-1;
	int p, nodeCount, partitionCount;

	int NLSIZE=7;
	double HIST_PROB=0.8;

	LockManager lm;
	private Logger LOG = Logger.getLogger(ServerTxn.class);

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
			appender = new FileAppender(layout,"worker"+wid+"_"+curTime+".log",false);
		} catch (IOException e) {
			e.printStackTrace();
		}    
		LOG.addAppender(appender);
		LOG.setLevel((Level) Level.INFO);

		LOG.debug("Instance initialization started");
		Config cfg = new ClasspathXmlConfig("hazelcast.xml");
		ManagementCenterConfig managementCenterConfig = new ManagementCenterConfig();
		managementCenterConfig.setEnabled(true);
		managementCenterConfig.setUrl("http://localhost:8080/mancenter");
		cfg.setManagementCenterConfig(managementCenterConfig);
		cfg.getGroupConfig().setName("3.0");

		cfg.setInstanceName(wid);
		cfg.getSerializationConfig().addPortableFactory(1, new PortableVertexFactory());

		MapConfig mapCfg = new MapConfig();
		mapCfg.setName("vertex");
		mapCfg.setBackupCount(0);
		cfg.addMapConfig(mapCfg);

		QueueConfig qCfg = new QueueConfig();
		qCfg.setName("queue");
		qCfg.setBackupCount(0);
		cfg.addQueueConfig(qCfg);

		System.setProperty("hazelcast.partition.count", numWorkers+"");

		instance = Hazelcast.newHazelcastInstance(cfg);

		IAtomicLong idGenerator = instance.getAtomicLong("worker-ids");
		myID = (int) idGenerator.getAndIncrement();
		myID= Integer.parseInt(wid);

	}

	public void waitMessage(String message, int limit){
		mes = message;
		System.out.println("Waiting for synchronization: "+limit);
		ITopic<String> topic1 = instance.getTopic("syncher");
		latch1 = new CountDownLatch(limit);
		topic1.addMessageListener(new MessageListener<String>() {
			public void onMessage(Message<String> msg) {
				LOG.debug("Message is: "+msg.getMessageObject());
				if(msg.getMessageObject().equals(mes)){
					latch1.countDown();
					System.out.println("Latch: "+latch1.getCount());
				}
			}
		});
		try {
			latch1.await();
		} catch (InterruptedException e) {e.printStackTrace();}
		LOG.debug("Graph loaded.");
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
						partitionID = jsonVertex.getInt(2) % clientP.numWorkers;
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
							int pp = (new Random()).nextInt(numWorkers);
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

	public static int findP(int ID, int nodeCount, int partitionCount){
		double sqrtPC=(Math.floor(Math.sqrt(partitionCount)));
		double pn = Math.floor(Math.sqrt(nodeCount)/sqrtPC);
		int column = (int) (ID%Math.floor(Math.sqrt(nodeCount)));
		int row = (int) (Math.floor(ID/Math.sqrt(nodeCount)));
		int p = (int) (Math.floor(row/pn)*sqrtPC + Math.floor(column/pn));
		return p % numWorkers;
	}

	boolean sample = false;
	public int compute(TransactionContext context, TransactionOptions options, 
			Vertex vertex, IAtomicLong opCtr, boolean isLazy){

		//		logger.info(getTime()+" Compute started");

		TransactionalMap<PAKey, Vertex> vertexMap = null;
		context = instance.newTransactionContext(options);
		context.beginTransaction();
		vertexMap = context.getMap("vertex");
		opCtr.incrementAndGet();

		HashSet<Integer> leaseMap;

		//		logger.info(getTime()+" nList is being calculated..");
		try {    
			//			nList = NeighborSelector.selectWeighted(nodeCount, wNum, myID, NLSIZE);
						nList = NeighborSelector.selectHistorical(nodeCount, numWorkers, myID, NLSIZE, nList, HIST_PROB);
//			nList = NeighborSelector.selectML(nodeCount, numWorkers, myID, NLSIZE, nList, HIST_PROB);
			LOG.info(getTime()+" nList is: "+nList);

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
								LOG.info(getTime()+" Read from lazy cache "+nID);
								cacheCount++;
								found = true;
							}
						}
					}

					if(!found){
						if(leaseMap.contains(nID)){
							if(lm.vertexCache.get(nID)==null){
								if(lm.txnCache.containsKey(nID)){
									nVertex = lm.txnCache.get(nID);
								}else{
									nVertex = vertexMap.get(key);
								}
								lm.vertexCache.put(nID, nVertex);
								LOG.info(getTime()+" Read normal-write to cache "+nID);
								cacheCount++;
							}else{
								nVertex = lm.vertexCache.get(nID);
								LOG.info(getTime()+" Read from vertex cache "+nID);
								cacheCount++;
							}
						}else{
							if(lm.txnCache.containsKey(nID)){
								nVertex = lm.txnCache.get(nID);
								LOG.info(getTime()+" Read from txnCache "+nID);
							}else{
								nVertex = vertexMap.get(key);
							}							
							if(!isLazy){ 
								vertexMap.put(key, nVertex);
								LOG.info(getTime()+" Wrote normally(!lazy) "+nID);
							}
							nocacheCount++;
						}
					}
				}
				else{
					if(lm.txnCache.containsKey(nID)){
						nVertex = lm.txnCache.get(nID);
					}else{
						nVertex = vertexMap.get(key);
					}
					vertexMap.put(key, nVertex);
					LOG.info(getTime()+" Read and wrote normally "+nID);
					nocacheCount++;
				}

				if(nVertex==null){
					if(lm.txnCache.containsKey(nID)){
						nVertex = lm.txnCache.get(nID);
					}else{
						nVertex = vertexMap.get(key);
					}
				}

				if(useCache && isLazy){ 
					synchronized(lm.lazyCache){
						lm.lazyCache.put(nID, nVertex);
					}				
				}
			}

			context.commitTransaction();
			LOG.info(getTime()+" Committed "+nList);

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

	/**
	 * args[0] is wid: worker id. It must start from 0 and end at numWorkers-1 <br>
	 * args[1] is numWorkers: number of workers excluding the client.Make it 2 4 8 16 etc.. <br>
	 * args[2] is TOTAL_OPS: number of total txn to finish <br>
	 * args[3] is NLSIZE: number of data items in a transaction <br>
	 * args[4] is HIST_PROB: probability of selecting the same item from the previous txn <br>
	 * args[5] is lazy or nolazy: enable or disable lazy unlocking <br>
	 * args[6] is cache or nocache: enable or disable caching of data before reading items <br>
	 */
	public static void main(String[] args) {    

		ServerTxn<PAKey> server = new ServerTxn<PAKey>();
		ParamHolder ph = new ParamHolder();
		
		String argsf[] = null;
	    try {
	    	BufferedReader br = new BufferedReader(new FileReader("../args.txt"));
			argsf = br.readLine().trim().split("\\s+");
		} catch (IOException e) {
			e.printStackTrace();
		}

		if(argsf.length>1) server.numWorkers = Integer.parseInt(argsf[1]);
		int status=0;
		wid=args[0];
		int TOTAL_OPS = Integer.parseInt(argsf[2]);
		server.NLSIZE = Integer.parseInt(argsf[3]);
		server.HIST_PROB = Double.parseDouble(argsf[4]);
		boolean isLazy = false;
		if(argsf.length>5){
			if(argsf[5].equals("lazy")){ isLazy=true;}
			else if(argsf[5].equals("nolazy")){ isLazy=false;}
		}
		if(argsf.length>6){
			if(argsf[6].equals("cache")) server.useCache = true;
			if(argsf[6].equals("nocache")) server.useCache = false;
		}	
		
		if(argsf.length>8){
			if(argsf[8].equals("zmq")) LockManager.zeroMQ = true;
			if(argsf[8].equals("nozmq")) LockManager.zeroMQ = false;
		}
		
		isRandom = false;

		server.initInstance();

		TransactionOptions options = new TransactionOptions().setTransactionType(
				TransactionType.TWO_PHASE);
		options.setTimeout(server.NLSIZE*10+10000, TimeUnit.MILLISECONDS);
		TransactionContext context = null;//instance.newTransactionContext(options);
		IMap<PAKey, Vertex> vertexMapI = null;

		server.waitMessage("Loaded",1);

		IAtomicLong opCtr = instance.getAtomicLong("operationCounter");
		vertexMapI = instance.getMap("vertex");
		server.nodeCount = vertexMapI.size();
		server.partitionCount = server.nodeCount <= 64 ? 4 : 16;
		ph.nodeCount = server.nodeCount;
		ph.partitionCount = server.partitionCount;

		server.LOG.debug("vertexMapI size is:"+server.nodeCount);

		NeighborSelector.initPrMap(server.nodeCount);

		boolean isBatch = true;
		if(isBatch){
			//			clientP.lm = new WorkerLockManagerOrdered(
			//					instance, clientP.myID, clientP.logger, isLazy, sTime);
			if(LockManager.zeroMQ)
				server.lm = new WorkerLockManagerStagedJeroMQ(
						instance, server.myID, server.LOG, isLazy, sTime, ph);
			else
				server.lm = new WorkerLockManagerStaged(
						instance, server.myID, server.LOG, isLazy, sTime, ph);
			}else{
			server.lm = new WorkerLockManager(
					instance, server.myID, server.LOG, isLazy, sTime);
		}
		if(argsf.length>7){
			if(argsf[7].equals("ml")) LockManager.useML = true;
			if(argsf[7].equals("noml")) LockManager.useML = false;
		}
		System.out.println("useML is: "+LockManager.useML);

		server.LOG.info(args);
		//		server.LOG.info("Local vertex list is: "+vertexMapI.localKeySet());
		CommonTools.sleep(500);


		long startTime = System.nanoTime();    
		boolean endOfWarmup = false;

		int opCounter = 0;
		if (server.myID>-1){
			while(opCtr.get()<TOTAL_OPS){
				Vertex vertex = null;
				double computeStartTime = System.nanoTime();
				status = server.compute(context, options, vertex, opCtr, isLazy);
				double computeElapsedTime = (System.nanoTime() - computeStartTime)/(Math.pow(10, 6));
				server.LOG.info(server.getTime()+" "+opCtr.get()+" Compute time is:"+computeElapsedTime);

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
						server.LOG.info(globalOps+" Elapsed time is:"+elapsedTime);
						System.out.println(globalOps+" Elapsed time is:"+elapsedTime);
					}
				}
				if(status==-1){
					break;
				}
				//				clientP.logger.info(clientP.getTime()+" Compute loop ends");

			}
		}
		double elapsedTime = (System.nanoTime() - startTime)/(Math.pow(10, 9));
		server.LOG.info(opCtr.get()+" Total time is:"+elapsedTime);
		System.out.println(opCtr.get()+" Total time is:"+elapsedTime);
		server.LOG.info("Rollback count is: "+rbctr);
		server.LOG.info(" Cache rate is: "+cacheCount+"/"+nocacheCount);
		System.out.println(" Cache rate is: "+cacheCount+"/"+nocacheCount);
		server.LOG.info("LocalTxnCtr is: "+server.lm.localTxnCtr);


		IAtomicLong finishedCtr = instance.getAtomicLong("finishedCtr");
		ITopic<String> topic1 = instance.getTopic("syncher");
		long t = finishedCtr.getAndIncrement();
		topic1.publish("Finished");
		if(t!=server.numWorkers+1)
		{
			server.waitMessage("Finished",(int) (server.numWorkers-t));
		}
		CommonTools.sleep(1000);
		instance.getLifecycleService().shutdown();
		System.exit(0);
	}
}
