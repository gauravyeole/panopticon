package seref;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;

import org.json.JSONArray;
import org.json.JSONException;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.IdGenerator;

public class ClientPA<T> {

	private HazelcastInstance instance;
	private int wNum=-1;

	public void initClient(String ip){
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName("dev").setPassword("dev-pass");
        clientConfig.addAddress(ip);
        clientConfig.getGroupConfig().setName("3.0");
        clientConfig.getSerializationConfig().addPortableFactory(1, new PortableVertexFactory());
        instance = HazelcastClient.newHazelcastClient(clientConfig);
        wNum = instance.getCluster().getMembers().size();
	}

	public int readInput(String path){
        IMap<T, Vertex> vertexMap = instance.getMap("vertex");
		File folder = new File(path);
		File[] listOfFiles = folder.listFiles();
		boolean isPartitioned = path.contains("MeshP");
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
						vertexId = jsonVertex.getInt(0);
						vertex.setVertexID(vertexId);
						vertex.setValue(jsonVertex.getDouble(1));
						JSONArray jsonEdgeArray;
						if(isPartitioned){
							partitionID = jsonVertex.getInt(2) % wNum;
							vertex.setPartitionID(partitionID);
							jsonEdgeArray = jsonVertex.getJSONArray(3);
						}else{
							jsonEdgeArray = jsonVertex.getJSONArray(2);   
						}
						for (int j = 0; j < jsonEdgeArray.length(); ++j) {
							JSONArray jsonEdge = jsonEdgeArray.getJSONArray(j);
							vertex.getOutNeighbors().add(new Integer(jsonEdge.getInt(0)));
						}
						Collections.sort(vertex.getOutNeighbors());
//						for (int j = 0; j < 1000; ++j) {
//							vertex.getDummyList().add(new Long(j));
//						}
						
						if(isPartitioned){
							T t = (T) new PAKey(vertexId, partitionID+"");
							vertexMap.put(t, vertex);
							IMap<Integer, T> partitionMap = instance.getMap("partitionMap");
							partitionMap.put(vertexId, t);
						}else{
							T t = (T) new Integer(vertex.getVertexID());
							vertexMap.put(t, vertex);
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
		return vertexMap.size();
	}
    public static void main(String[] args) {
    	ClientPA<PAKey> clientP = new ClientPA<PAKey>();
    	ClientPA<Integer> clientI = new ClientPA<Integer>();
    	ClientPA myClient = null;
    	
    	String cl = args[2];
    	Class clazz = Integer.class;
    	if(cl.equals("pakey")){
    		clazz = PAKey.class;
    		myClient = clientP;
    	}else if(cl.equals("integer")){
    		clazz = PAKey.class;
    		myClient = clientI;
    	}
    	
    	
    	myClient.initClient(args[0]);
    	int size = myClient.readInput("../input/"+args[1]);
		System.out.println("Map Size:" + size);
		
		IAtomicLong idGenerator = myClient.instance.getAtomicLong("worker-ids");
		idGenerator.set(0);

		ITopic<String> topic1 = myClient.instance.getTopic("syncher");
		topic1.publish("Loaded");
		
		CommonTools.waitTermination(myClient.instance, myClient.wNum);
		topic1.publish("Loaded");

		
		myClient.instance.getLifecycleService().shutdown();

    }
}
