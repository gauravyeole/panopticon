package seref;
import com.hazelcast.config.Config;
import com.hazelcast.config.ManagementCenterConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
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

public class ServerT {

	private static HazelcastInstance instance;
	private static String name = "-1";
	private static int rbctr = 0;

	public static void initInstance(){
		Config cfg = new ClasspathXmlConfig("hazelcast.xml");
		ManagementCenterConfig managementCenterConfig = new ManagementCenterConfig();
		managementCenterConfig.setEnabled(true);
		managementCenterConfig.setUrl("http://localhost:8080/mancenter");
		cfg.setManagementCenterConfig(managementCenterConfig);
		cfg.getGroupConfig().setName("3.0");

		cfg.setInstanceName(name);
		cfg.getSerializationConfig().addPortableFactory(1, new PortableVertexFactory());
		instance = Hazelcast.newHazelcastInstance(cfg);
	}

	public static void waitLoading(){
		System.out.println("Waiting for graph loading..");
		ITopic<String> topic1 = instance.getTopic("syncher");
		final CountDownLatch latch1 = new CountDownLatch(1);
		topic1.addMessageListener(new MessageListener<String>() {
			public void onMessage(Message<String> msg) {
				System.out.println("Message is: "+msg.getMessageObject());
				if(msg.getMessageObject().equals("Loaded")){
					latch1.countDown();
				}
			}
		});
		try {
			latch1.await();
		} catch (InterruptedException e) {e.printStackTrace();}
		System.out.println("Graph loaded.");
	}

	public static void readInput(String path, TransactionContext context, TransactionOptions options){
		File folder = new File(path);
		File[] listOfFiles = folder.listFiles();
		TransactionalMap<Integer, Vertex> vertexMap;// = context.getMap("vertex");
		TransactionalQueue<Integer> queue;// = context.getQueue("queue");
		TransactionalMap<Integer, Integer> queueMap = null;

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

						JSONArray jsonEdgeArray = jsonVertex.getJSONArray(2);          
						for (int j = 0; j < jsonEdgeArray.length(); ++j) {
							JSONArray jsonEdge = jsonEdgeArray.getJSONArray(j);
							vertex.getOutNeighbors().add(new Integer(jsonEdge.getInt(0)));
						}
						Collections.sort(vertex.getOutNeighbors());

						context = instance.newTransactionContext(options);
						context.beginTransaction();
						queue = context.getQueue("queue");
						queueMap = context.getMap("queueMap");
						vertexMap = context.getMap("vertex");

						try{
							Integer id = vertex.getVertexID();
							vertexMap.put(id, vertex);
							queue.offer(id);
							queueMap.put(id, id);
							context.commitTransaction();			
						}catch(Throwable t){
							context.rollbackTransaction();
						}

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

	public static void compute(TransactionContext context, TransactionOptions options, 
			Vertex vertex, IAtomicLong opCtr){

//		System.out.println("SEREF--------------: entered compute ");

		TransactionalMap<Integer, Vertex> vertexMap = null;
		TransactionalQueue<Integer> queue = null;
		TransactionalMap<Integer, Integer> queueMap = null;
		context = instance.newTransactionContext(options);
		context.beginTransaction();
		queue = context.getQueue("queue");
		vertexMap = context.getMap("vertex");
		queueMap = context.getMap("queueMap");

		int nextColor = -1;
		int curColor = (int) vertex.getValue();
		try {    
			int[] colorMap = new int[2*vertex.getOutNeighbors().size()];
			for(int i=0; i<colorMap.length; i++){ colorMap[i]=0;}
			//System.out.println(vertex.getVertexID()+" -> "+ vertex.getValue()+":");
			for(Integer neighbor : vertex.getOutNeighbors()){
				//System.out.println("Reading "+neighbor);
				int nColor = (int) vertexMap.get(neighbor).getValue();
				if(nColor < colorMap.length){
					colorMap[nColor]=1;
				}
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
				vertexMap.put(vertex.getVertexID(), vertex);
//				System.out.println("SEREF--------------: P2 ");

				for(Integer neighbor : vertex.getOutNeighbors()){
					if(queueMap.get(neighbor)==null){
//						System.out.println("SEREF--------------: P3 ");

						queue.offer(neighbor);
						queueMap.put(neighbor, neighbor);
//						System.out.println("SEREF--------------: P4 ");

					}
				}
			}
			context.commitTransaction();
//			System.out.println("SEREF--------------: P5 ");

			long ctr = opCtr.incrementAndGet();
			//System.out.println("Committed "+vertex.getVertexID());
			//System.out.println("Operation count is "+ctr);
		}catch (Throwable t)  {
			//System.out.println("Rollback "+vertex.getVertexID());
			context.rollbackTransaction();
			rbctr++;
			try {
				Thread.sleep(new Random().nextInt(100));
			} catch (InterruptedException e) {e.printStackTrace();}

			context = instance.newTransactionContext(options);
			context.beginTransaction();
			queue = context.getQueue("queue");
			queueMap = context.getMap("queueMap");
			try{
				Integer id = vertex.getVertexID();
				queue.offer(id);
				queueMap.put(id, id);
				context.commitTransaction();
			}catch(Throwable t2){
				context.rollbackTransaction();
			}
		}
	}

	public static void main(String[] args) {    
		name=args[0];
		initInstance();
		TransactionOptions options = new TransactionOptions().setTransactionType(TransactionType.LOCAL);
		options.setTimeout(10, TimeUnit.SECONDS);
		TransactionContext context = null;//instance.newTransactionContext(options);
		TransactionalMap<Integer, Vertex> vertexMap = null;
		TransactionalMap<Integer, Integer> queueMap = null;
		TransactionalQueue<Integer> queue = null;

		if(instance.getName().equals("1")){
			readInput("../input/"+args[1], context, options);
			ITopic<String> topic1 = instance.getTopic("syncher");
			topic1.publish("Loaded");
		}else{
			waitLoading();
		}

		long startTime = System.nanoTime();    

		IAtomicLong counter = instance.getAtomicLong("counter");
		IAtomicLong opCtr = instance.getAtomicLong("operationCounter");

//		CommonTools.getInput();
		int size = -1;
		context = instance.newTransactionContext(options);
		context.beginTransaction();
		queue = context.getQueue("queue");
		try{
			size = queue.size();
			context.commitTransaction();
		}catch(Throwable t){
			context.rollbackTransaction();
		}
		System.out.println("Queue size is:"+size);

		while(size>0 || counter.get()>0 ){
//			CommonTools.getInput();

			Integer key = null;
			context = instance.newTransactionContext(options);
			context.beginTransaction();
			queue = context.getQueue("queue");
			queueMap = context.getMap("queueMap");
			try{
				key = queue.poll();
//				System.out.println("SEREF--------------: "+key);
				queueMap.delete(key);
				context.commitTransaction();
			}catch(Throwable t){
				context.rollbackTransaction();
			}

			if(key!=null){
				counter.incrementAndGet();
				context = instance.newTransactionContext(options);
				context.beginTransaction();
				vertexMap = context.getMap("vertex");
				Vertex vertex = null;
				try{
					vertex = vertexMap.get(key);
					context.commitTransaction();
				}catch(Throwable t){
					context.rollbackTransaction();
				}

				compute(context, options, vertex, opCtr);

				counter.decrementAndGet();
			}
			if(opCtr.get()%1000==0){
				double elapsedTime = (System.nanoTime() - startTime)/(Math.pow(10, 9));
				System.out.println(opCtr.get()+" Elapsed time is:"+elapsedTime);
			}
			context = instance.newTransactionContext(options);
			context.beginTransaction();
			queue = context.getQueue("queue");
			queueMap = context.getMap("queueMap");
			try{
				size = queue.size();
				context.commitTransaction();
			}catch(Throwable t){
				context.rollbackTransaction();
			}
			//System.out.println("Queue size is:"+size);
			//System.out.println("QueueMap size is:"+queueMap.size());
		}

		double elapsedTime = (System.nanoTime() - startTime)/(Math.pow(10, 9));
		System.out.println(opCtr.get()+" Total time is:"+elapsedTime);
		
//		context = instance.newTransactionContext(options);
//		context.beginTransaction();
//		vertexMap = context.getMap("vertex");
//		for (int g=0; g<vertexMap.size(); g++){
//			System.out.println("Vertex is: "+g+" -> "+ vertexMap.get(new Integer(g)).getValue());
//		}
//		context.commitTransaction();

		instance.getLifecycleService().shutdown();
	}
}
