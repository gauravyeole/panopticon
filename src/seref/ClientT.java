package seref;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.json.JSONArray;
import org.json.JSONException;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionOptions.TransactionType;

public class ClientT {

	private static HazelcastInstance client;

	public static void initClient(String ip){
		ClientConfig clientConfig = new ClientConfig();
		clientConfig.getGroupConfig().setName("dev").setPassword("dev-pass");
		clientConfig.addAddress(ip+":5701");
		clientConfig.getGroupConfig().setName("3.0");
		clientConfig.getSerializationConfig().addPortableFactory(1, new PortableVertexFactory());
		client = HazelcastClient.newHazelcastClient(clientConfig);
	}

	public static void readInput(String path, TransactionContext context, TransactionOptions options){
		File folder = new File(path);
		File[] listOfFiles = folder.listFiles();
		TransactionalMap<Integer, Vertex> vertexMap;// = context.getMap("vertex");
		TransactionalQueue<Integer> queue;// = context.getQueue("queue");
		TransactionalMap<Integer, Integer> queueMap = null;

		
		context = client.newTransactionContext(options);
		context.beginTransaction();
		queue = context.getQueue("queue");
		queueMap = context.getMap("queueMap");
		vertexMap = context.getMap("vertex");

		try{
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

							Integer id = vertex.getVertexID();
							vertexMap.put(id, vertex);
							queue.offer(id);
							queueMap.put(id, id);

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

			context.commitTransaction();			
		}catch(Throwable t){
			context.rollbackTransaction();
		}
	}
	public static void main(String[] args) {
		initClient(args[0]);

		TransactionOptions options = new TransactionOptions().setTransactionType(TransactionType.LOCAL);
		options.setTimeout(10, TimeUnit.SECONDS);
		TransactionContext context = null;//client.newTransactionContext(options);

		TransactionalMap<Integer, Vertex> vertexMap;
		TransactionalQueue<Integer> queue;

		readInput("../input/"+args[1],context, options);

		context = client.newTransactionContext(options);
		context.beginTransaction();
		queue = context.getQueue("queue");
		vertexMap = context.getMap("vertex");

		try{
			System.out.println("Map Size:" + vertexMap.size());
			System.out.println("Queue size is:"+queue.size());
			context.commitTransaction();
		}catch(Throwable t){
			context.rollbackTransaction();
		}
		ITopic<String> topic1 = client.getTopic("syncher");
		topic1.publish("Loaded");
	}
}
