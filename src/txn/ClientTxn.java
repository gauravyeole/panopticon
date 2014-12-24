package txn;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.Client;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Member;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.*;
import org.json.JSONArray;
import org.json.JSONException;

import seref.CommonTools;
import seref.PAKey;
import seref.PortableVertexFactory;
import seref.Vertex;

/**
 * @author aeyate
 * This class creates a Hz client which connects to the first Hz instance in the cluster
 * and runs the broker 
 * @param <T>
 */
public class ClientTxn<T> {

	private static HazelcastInstance instance;
	private static String name = "-1";
	private static boolean isRandom = false;
	private static String mes = "";
	private static  CountDownLatch latch1;

	private Integer myID = -1;
	PAKey key, key2;
	private int numWorkers=-1;
	int p, nodeCount, partitionCount;

	LockManager lm;
//	MasterLockManagerOrdered lm;
//	MasterLockManagerStagedJeroMQ lm;
//	MasterLockManagerStaged lm;

	private Logger logger = Logger.getLogger(ServerTxn.class);

	/**
	 * This method initializes the logger and client. Then it creates the client instance
	 * @param ip is the ip:port of the hz cluster machine to connect this client to
	 * 
	 */
	public void initInstance(String ip){
    	SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
    	String curTime = sdf.format(Calendar.getInstance().getTime());

		myID= -1;
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
       
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName("dev").setPassword("dev-pass");
        clientConfig.addAddress(ip);
        clientConfig.getGroupConfig().setName("3.0");
        clientConfig.getSerializationConfig().addPortableFactory(1, new PortableVertexFactory());
        instance = HazelcastClient.newHazelcastClient(clientConfig);

	}

	public void waitMessage(String message, int limit){
		mes = message;
		System.out.println("Waiting for synchronization: "+limit);
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

	public void readInput(String path){
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
						partitionID = jsonVertex.getInt(2) % this.numWorkers;
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
//						logger.debug("Inserted vertex: "+t);
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

	/**
	 * arg[0] is ip:portjavadoc <br>
	 * arg[1] is number of workers <br>
	 * args[2] is input name <br>
	 * args[3] is xConsecutive: # of conseq requests to get the lease <br>
	 */
	public static void main(String[] args) {    
		ClientTxn<PAKey> broker = new ClientTxn<PAKey>();

		broker.numWorkers = Integer.parseInt(args[1]);

		Integer xConsecutive = 2;
		if(args.length>3) xConsecutive = Integer.parseInt(args[3]);

		broker.initInstance("localhost:"+args[0].split(":")[1]);
		broker.readInput("../input/"+args[2]);
		ITopic<String> topic1 = instance.getTopic("syncher");
		topic1.publish("Loaded");

		//		CommonTools.getInput();
		IMap<PAKey, Vertex> vertexMapI = instance.getMap("vertex");
		broker.nodeCount = vertexMapI.size();
		broker.logger.debug("vertexMapI size is:"+broker.nodeCount);
		broker.logger.info(args);

		if(args.length>5){
			if(args[5].equals("zmq")) LockManager.zeroMQ = true;
			if(args[5].equals("nozmq")) LockManager.zeroMQ = false;
		}

//		broker.lm = new MasterLockManagerOrdered(instance, broker.myID, broker.logger, xConsecutive);
		if(LockManager.zeroMQ)
			broker.lm = new MasterLockManagerStagedJeroMQ(instance, broker.myID, broker.logger, xConsecutive, args[0]);
		else
			broker.lm = new MasterLockManagerStaged(instance, broker.myID, broker.logger, xConsecutive);
		
		if(args.length>4){
			if(args[4].equals("ml")) LockManager.useML = true;
			else if(args[4].equals("noml")) LockManager.useML = false;
		}
//		System.out.println("useML is: "+LockManager.useML);
		
		for(int i=0; i<broker.nodeCount; i++){
			broker.lm.getLockMap().put(i, -1);
		}
//		for(int i=0; i<broker.nodeCount; i++){
//			broker.logger.debug("Node is:"+i+" "+broker.lm.getLockMap().get(i));
//		}
		CommonTools.sleep(100);

		broker.partitionCount = broker.nodeCount <= 64 ? 4 : 16;

		IAtomicLong finishedCtr = instance.getAtomicLong("finishedCtr");
		long t = finishedCtr.get();
		if(t!=broker.numWorkers){
			broker.waitMessage("Finished",(int) (broker.numWorkers-t));
		}
		System.out.println("Broker has finished: "+t);
		topic1.publish("Finished");
		broker.lm.cleanup();
		
		instance.getLifecycleService().shutdown();
		System.exit(0);

	}
}
