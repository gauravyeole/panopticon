package txn;
import com.hazelcast.config.Config;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.ManagementCenterConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Member;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.nio.Address;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionOptions.TransactionType;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
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

public class ServerTxnHz<T> {

	private static HazelcastInstance instance;
	private static String name = "-1";
	private static boolean isRandom = true;
	private static int rbctr = 0;
	private static String mes = "";
	private static  CountDownLatch latch1;
	private ArrayList<Integer> nList=null;

	private Integer myID = -1;
	PAKey key, key2;
	private int wNum = -1;
	int p, nodeCount, partitionCount;

	int NLSIZE=7;
	double HIST_PROB=0.8;
	//	LockManager lm;
	private Logger logger = Logger.getLogger(ServerTxnHz.class);
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

		logger.debug("Instance initialization started "+name);
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
		System.out.println("Waiting for graph loading..");
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

	public void readInput(ServerTxnHz<PAKey> clientP, String path){
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

	public int compute(TransactionContext context, TransactionOptions options, 
			Vertex vertex, IAtomicLong opCtr){

		//		logger.debug("SEREF--------------: entered compute ");

		TransactionalMap<PAKey, Vertex> vertexMap = null;
		TransactionalQueue<PAKey> queue = null;
		TransactionalMap<PAKey, Integer> queueMap = null;
		context = instance.newTransactionContext(options);
		context.beginTransaction();
		queue = context.getQueue("queue");
		vertexMap = context.getMap("vertex");
		queueMap = context.getMap("queueMap");
		opCtr.incrementAndGet();


		try {    
			//			nList = NeighborSelector.selectWeighted(nodeCount, wNum, myID, NLSIZE);
			nList = NeighborSelector.selectHistorical(nodeCount, wNum, myID, NLSIZE, nList, HIST_PROB);
			logger.debug("nList is: "+nList);

			for(int i=0; i<NLSIZE; i++){
				int nID = nList.get(i);
				p = findP(nID, nodeCount, partitionCount);
				key = new PAKey(nID, p+"");
				Vertex nVertex = vertexMap.get(key);
				vertexMap.put(key, nVertex);
				logger.info(getTime()+" Read and wrote "+nID);
			}
			//			CommonTools.getInput();
			//			lm.unlockAll(nList);

			context.commitTransaction();
			logger.info(getTime()+" Committed "+nList);

		}catch (Throwable t)  {
			//			lm.unlockAll(nList);
			t.printStackTrace();
			logger.debug("Rollback "+vertex.getVertexID()+" with error "+t);
			rbctr++;
			context.rollbackTransaction();
		}
		return 0;
	}

	public static void main(String[] args) {    
		ServerTxnHz<PAKey> clientP = new ServerTxnHz<PAKey>();

		if(args.length>2) clientP.wNum = Integer.parseInt(args[1]);

		int status=0;

		name=args[0];
		int TOTAL_OPS = Integer.parseInt(args[2]);
		clientP.NLSIZE = Integer.parseInt(args[3]);
		clientP.HIST_PROB = Double.parseDouble(args[4]);

		isRandom = false;

		clientP.initInstance();

		TransactionOptions options = new TransactionOptions().setTransactionType(
				TransactionType.TWO_PHASE);
		options.setTimeout(50000, TimeUnit.MILLISECONDS);
		TransactionContext context = null;//instance.newTransactionContext(options);

		IMap<PAKey, Vertex> vertexMapI = null;
		IQueue<PAKey> queueI = null;
		IMap<PAKey, Integer> queueMapI = null;

		Cluster cluster = instance.getCluster();
		IMap<Integer, InetSocketAddress> memberMap = instance.getMap("memberMap");
		memberMap.put(clientP.myID, cluster.getLocalMember().getInetSocketAddress());

		if(instance.getName().equals("0")){
			clientP.readInput(clientP, "../input/"+args[5]);
			ITopic<String> topic1 = instance.getTopic("syncher");
			topic1.publish("Loaded");
		}else{
			clientP.waitMessage("Loaded",1);
		}

		IAtomicLong counter = instance.getAtomicLong("counter");
		IAtomicLong opCtr = instance.getAtomicLong("operationCounter");

		//		CommonTools.getInput();
		vertexMapI = instance.getMap("vertex");
		clientP.nodeCount = vertexMapI.size();
		clientP.logger.debug("vertexMapI size is:"+clientP.nodeCount);

		clientP.logger.info("Local vertex list is: "+vertexMapI.localKeySet());
//		for(PAKey vertex : vertexMapI.localKeySet()){
//			clientP.logger.debug("Local vertex is:"+vertex);
//		}

		CommonTools.sleep(500);

		clientP.partitionCount = clientP.nodeCount == 16 ? 4 : 16;

		queueI = instance.getQueue("queue@"+clientP.myID);
		int qSize = queueI.size();
		clientP.logger.debug("Queue size is:"+qSize);
		if(qSize==0){
			int id = (new Random()).nextInt(clientP.nodeCount);
			int pid = clientP.findP(id,clientP.nodeCount,clientP.partitionCount);
			queueI.offer(new PAKey(id,pid+""));
		}

		long startTime = System.nanoTime();    
		sTime = System.nanoTime();

		int opCounter = 0;
		if (clientP.myID>0)
			while(opCtr.get()<TOTAL_OPS && (qSize>0 || counter.get()>0) ){
				//			CommonTools.getInput();

				PAKey key = null;
				queueI = instance.getQueue("queue@"+clientP.myID);
				queueMapI = instance.getMap("queueMap");
				key = queueI.poll();
				//			logger.debug("SEREF--------------: "+key);			

				if(key!=null){
					//				queueMapI.delete(key);
					queueI.offer(key);
					counter.incrementAndGet();
					Vertex vertex = null;
					vertexMapI.lock(key);
					vertex = vertexMapI.get(key);
					vertexMapI.unlock(key);

					double computeStartTime = System.nanoTime();
					status = clientP.compute(context, options, vertex, opCtr);
					double computeElapsedTime = (System.nanoTime() - computeStartTime)/(Math.pow(10, 6));
					clientP.logger.info(opCtr.get()+" Compute time is:"+computeElapsedTime);

					counter.decrementAndGet();
				}
				long globalOps = opCtr.get();
				if(globalOps/100 != opCounter || status == -1){
					opCounter = (int) (globalOps/100);
					double elapsedTime = (System.nanoTime() - startTime)/(Math.pow(10, 9));
					clientP.logger.info(opCtr.get()+" Elapsed time is:"+elapsedTime);
					System.out.println(opCtr.get()+" Elapsed time is:"+elapsedTime);
				}
				if(status==-1){
					break;
				}
				qSize = queueI.size();
				//logger.debug("Queue size is:"+size);
				//logger.debug("QueueMap size is:"+queueMap.size());
			}

		if (clientP.myID>0){
			PrintStream ps = new PrintStream(new FileOutputStream(FileDescriptor.out));
			System.setOut(ps);
		}

		double elapsedTime = (System.nanoTime() - startTime)/(Math.pow(10, 9));
		clientP.logger.info(opCtr.get()+" Total time is:"+elapsedTime);
		System.out.println(opCtr.get()+" Total time is:"+elapsedTime);
		clientP.logger.debug("Rollback count is:"+rbctr);


		//		vertexMapI = instance.getMap("vertex");
		//		for (int g=0; g<vertexMapI.size(); g++){
		//			int p = clientP.findP(g, clientP.nodeCount, clientP.partitionCount);
		//			logger.debug("Reading vertex: "+new PAKey(g, p+""));
		//			logger.debug("Vertex is: "+g+" -> "+ vertexMapI.get(new PAKey(g, p+"")).getValue());
		//		}

		IAtomicLong finishedCtr = instance.getAtomicLong("finishedCtr");
		long t = finishedCtr.incrementAndGet();
		ITopic<String> topic1 = instance.getTopic("syncher");
		if(t!=clientP.wNum){
			clientP.waitMessage("Finished",(int) (clientP.wNum-t));
		}
		topic1.publish("Finished");

		instance.getLifecycleService().shutdown();
	}
}
