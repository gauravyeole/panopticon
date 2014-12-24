package seref;
import com.hazelcast.config.Config;
import com.hazelcast.config.ExecutorConfig;
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
import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionOptions.TransactionType;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.json.JSONArray;
import org.json.JSONException;

public class ServerTPA<T> {

	private static HazelcastInstance instance;
	private static String name = "-1";
	private static boolean isRandom = true;
	private static int rbctr = 0;
	private static String mes = "";
	private static  CountDownLatch latch1;
	
	private long myID = -1;
	PAKey key, key2;
	private int wNum=-1;
	int p, nodeCount, partitionCount;


	public void initInstance(){
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
						
		System.setProperty("hazelcast.partition.count", "2");

		instance = Hazelcast.newHazelcastInstance(cfg);
		
		IAtomicLong idGenerator = instance.getAtomicLong("worker-ids");
		myID = idGenerator.getAndIncrement();
		myID= Integer.parseInt(name);
		
	}

	public void waitMessage(String message, int limit){
		mes = message;
		System.out.println("Waiting for graph loading..");
		ITopic<String> topic1 = instance.getTopic("syncher");
		latch1 = new CountDownLatch(limit);
		topic1.addMessageListener(new MessageListener<String>() {
			public void onMessage(Message<String> msg) {
				System.out.println("Message is: "+msg.getMessageObject());
				if(msg.getMessageObject().equals(mes)){
					latch1.countDown();
				}
			}
		});
		try {
			latch1.await();
		} catch (InterruptedException e) {e.printStackTrace();}
		System.out.println("Graph loaded.");
	}

	public void readInput(ServerTPA<PAKey> clientP, String path){
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
//							System.out.println("Inserted vertex: "+t);
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

//		System.out.println("SEREF--------------: entered compute ");

		TransactionalMap<PAKey, Vertex> vertexMap = null;
		TransactionalQueue<PAKey> queue = null;
		TransactionalQueue<PAKey> nqueue = null;
		TransactionalMap<PAKey, Integer> queueMap = null;
		context = instance.newTransactionContext(options);
		context.beginTransaction();
		queue = context.getQueue("queue");
		vertexMap = context.getMap("vertex");
		queueMap = context.getMap("queueMap");

		int nextColor = -1;
		int curColor = -1;
		try{
			curColor = (int) vertex.getValue();
		}catch(Throwable t){
			context.rollbackTransaction();
			return -1;
		}
		try {    
			int[] colorMap = new int[2*vertex.getOutNeighbors().size()];
			for(int i=0; i<colorMap.length; i++){ colorMap[i]=0;}
//			System.out.println(vertex.getVertexID()+" -> "+ vertex.getValue()+":");
			for(Integer neighbor : vertex.getOutNeighbors()){
//				System.out.println("Reading "+neighbor);
				p = findP(neighbor, nodeCount, partitionCount);
				key = new PAKey(neighbor.intValue(), p+"");
				Vertex nVertex = vertexMap.get(key);
				vertexMap.put(key, nVertex);
				int nColor = (int) nVertex.getValue();
				if(nColor < colorMap.length){
					colorMap[nColor]=1;
				}
//				System.out.println("Reading neigbor "+key+" with color: "+nColor);
			}
			for(int i=0; i<colorMap.length; i++){
				if(colorMap[i]!=1){
					nextColor=i;
					//System.out.println("Next color is:"+nextColor);
					break;
				}
			}
			if(curColor != nextColor){
				vertex.setValue(nextColor);
				key2 = new PAKey(vertex.getVertexID(), vertex.getPartitionID()+"");
				vertexMap.put(key2, vertex);
//				System.out.println("SEREF--------------: P2 ");

				for(Integer neighbor : vertex.getOutNeighbors()){
					p = findP(neighbor, nodeCount, partitionCount);
					if(!isRandom){ 
						nqueue = context.getQueue("queue@"+p);
					}else{
						int pp = (new Random()).nextInt(wNum);
						nqueue = context.getQueue("queue@"+pp);
					}
					key = new PAKey(neighbor.intValue(), p+"");
					if(queueMap.get(neighbor)==null){
//						System.out.println("SEREF--------------: P3 ");

						nqueue.offer(key);
						queueMap.put(key, neighbor);
//						System.out.println("SEREF--------------: P4 ");

					}
				}
			}
			context.commitTransaction();
//			System.out.println("SEREF--------------: P5 ");

			
			//System.out.println("Committed "+vertex.getVertexID());
			//System.out.println("Operation count is "+ctr);
		}catch (Throwable t)  {
			System.out.println("Rollback "+vertex.getVertexID()+" with error "+t);
			rbctr++;
			context.rollbackTransaction();
			try {
				Thread.sleep(new Random().nextInt(10));
			} catch (InterruptedException e) {e.printStackTrace();}

			context = instance.newTransactionContext(options);
			context.beginTransaction();
			queue = context.getQueue("queue@"+vertex.getPartitionID());
			queueMap = context.getMap("queueMap");
			try{
				key2 = new PAKey(vertex.getVertexID(), vertex.getPartitionID()+"");
				queue.offer(key2);
				queueMap.put(key2, vertex.getVertexID());
				context.commitTransaction();
			}catch(Throwable t2){
				context.rollbackTransaction();
			}
		}
		long ctr = opCtr.incrementAndGet();
		return 0;
	}

	public static void main(String[] args) {    
		ServerTPA<PAKey> clientP = new ServerTPA<PAKey>();
    	
    	if(args.length>2) clientP.wNum = Integer.parseInt(args[1]);
    	
    	int status=0;
    	
		name=args[0];
		if(args[2].equals("p")){
			isRandom = false;
		}
		
		clientP.initInstance();
		
		TransactionOptions options = new TransactionOptions().setTransactionType(
				TransactionType.TWO_PHASE);
		options.setTimeout(50000, TimeUnit.MILLISECONDS);
		TransactionContext context = null;//instance.newTransactionContext(options);
		
		IMap<PAKey, Vertex> vertexMapI = null;
		IQueue<PAKey> queueI = null;
		IMap<PAKey, Integer> queueMapI = null;

		if(instance.getName().equals("0")){
			clientP.readInput(clientP, "../input/"+args[3]);
			ITopic<String> topic1 = instance.getTopic("syncher");
			topic1.publish("Loaded");
		}else{
			clientP.waitMessage("Loaded",1);
		}
		

		long startTime = System.nanoTime();    

		IAtomicLong counter = instance.getAtomicLong("counter");
		IAtomicLong opCtr = instance.getAtomicLong("operationCounter");

//		CommonTools.getInput();
		int qSize = -1;
		vertexMapI = instance.getMap("vertex");
		clientP.nodeCount = vertexMapI.size();
		System.out.println("vertexMapI size is:"+clientP.nodeCount);
		if(clientP.nodeCount == 16){
			clientP.partitionCount = 4;  //16vertex ise 4, diger gibi 16 olmali			
		}
		else{
			clientP.partitionCount = 16;  //16vertex ise 4, diger gibi 16 olmali
		}
		queueI = instance.getQueue("queue@"+clientP.myID);
		qSize = queueI.size();
		
		System.out.println("Queue size is:"+qSize);

		while(qSize>0 || counter.get()>0 ){
//			CommonTools.getInput();

			PAKey key = null;
			queueI = instance.getQueue("queue@"+clientP.myID);
			queueMapI = instance.getMap("queueMap");
			key = queueI.poll();
//			System.out.println("SEREF--------------: "+key);			

			if(key!=null){
				queueMapI.delete(key);
				counter.incrementAndGet();
				Vertex vertex = null;
				vertex = vertexMapI.get(key);

				status = clientP.compute(context, options, vertex, opCtr);

				counter.decrementAndGet();
			}
			if(opCtr.get()%1000==0 || status == -1){
				double elapsedTime = (System.nanoTime() - startTime)/(Math.pow(10, 9));
				System.out.println(opCtr.get()+" Elapsed time is:"+elapsedTime);
			}
			if(status==-1){
				break;
			}
			qSize = queueI.size();
			//System.out.println("Queue size is:"+size);
			//System.out.println("QueueMap size is:"+queueMap.size());
		}

		double elapsedTime = (System.nanoTime() - startTime)/(Math.pow(10, 9));
		System.out.println(opCtr.get()+" Total time is:"+elapsedTime);
		System.out.println("Rollback count is:"+rbctr);
		
		
//		vertexMapI = instance.getMap("vertex");
//		for (int g=0; g<vertexMapI.size(); g++){
//			int p = clientP.findP(g, clientP.nodeCount, clientP.partitionCount);
//			System.out.println("Reading vertex: "+new PAKey(g, p+""));
//			System.out.println("Vertex is: "+g+" -> "+ vertexMapI.get(new PAKey(g, p+"")).getValue());
//		}

		IAtomicLong finishedCtr = instance.getAtomicLong("finishedCtr");
		long t = finishedCtr.incrementAndGet();
		if(t==clientP.wNum){
			ITopic<String> topic1 = instance.getTopic("syncher");
			topic1.publish("Finished");
		}else{
			clientP.waitMessage("Finished",1);
		}
		instance.getLifecycleService().shutdown();
	}
}
