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
import com.hazelcast.core.IMap;
import com.hazelcast.core.ITopic;

public class Client {

	private static HazelcastInstance client;

	public static void initClient(String ip){
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName("dev").setPassword("dev-pass");
        clientConfig.addAddress(ip);
        clientConfig.getGroupConfig().setName("3.0");
        clientConfig.getSerializationConfig().addPortableFactory(1, new PortableVertexFactory());
        client = HazelcastClient.newHazelcastClient(clientConfig);
	}

	public static void readInput(String path, IMap<Integer, Vertex> vertexMap){
		File folder = new File(path);
		File[] listOfFiles = folder.listFiles();

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
//						for (int j = 0; j < 1000; ++j) {
//							vertex.getDummyList().add(new Long(j));
//						}
						
						vertexMap.put(vertex.getVertexID(), vertex);
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
    public static void main(String[] args) {
    	initClient(args[0]);

        IMap<Integer, Vertex> vertexMap = client.getMap("vertex");

		readInput("../input/"+args[1],vertexMap);
		System.out.println("Map Size:" + vertexMap.size());
		
		ITopic<String> topic1 = client.getTopic("syncher");
		topic1.publish("Loaded");
		
		client.getLifecycleService().shutdown();

    }
}
