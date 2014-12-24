package seref;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;

/**
 * 
 */

/**
 * @author aeyate
 *
 */
public class CommonTools {
	
	public static String mes = null;
	
	public static void getInput(){
		try { System.in.read();
		} catch (IOException e) {e.printStackTrace();}
	}

	public static void sleep(int millis){
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void waitLoading(HazelcastInstance instance){
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
	
	public static String waitQuery(HazelcastInstance instance){
		System.out.println("Waiting for updates to graph..");
		ITopic<String> topic1 = instance.getTopic("query");
		final CountDownLatch latch1 = new CountDownLatch(1);
		topic1.addMessageListener(new MessageListener<String>() {
			public void onMessage(Message<String> msg) {
				System.out.println("Message is: "+msg.getMessageObject());
				latch1.countDown();
				mes = msg.getMessageObject();
			}
		});
		try {
			latch1.await();
		} catch (InterruptedException e) {e.printStackTrace();}
		System.out.println("Query received.");
		return mes;
	}

	public static void waitTermination(HazelcastInstance instance, int wNum){
		System.out.println("Waiting for termination..");
		ITopic<String> topic1 = instance.getTopic("terminator");
		final CountDownLatch latch1 = new CountDownLatch(wNum);
		topic1.addMessageListener(new MessageListener<String>() {
			public void onMessage(Message<String> msg) {
				System.out.println("Message is: "+msg.getMessageObject());
				if(msg.getMessageObject().equals("Ready")){
					latch1.countDown();
				}
			}
		});
		try {
			latch1.await();
		} catch (InterruptedException e) {e.printStackTrace();}
		System.out.println("Termination started.");
	}
	
	public static void lockNeighborhood(IMap<Integer, Vertex> vertexMap, Vertex vertex){
		ArrayList<Integer> nn = vertex.getOutNeighbors();
		nn.add(vertex.getVertexID());
		Collections.sort(nn);
		for(Integer neighbor : nn){
			//System.out.println("Locking "+neighbor);
			vertexMap.lock(neighbor);
		}
		nn.remove(new Integer(vertex.getVertexID()));
	}
	public static void unLockNeighborhood(IMap<Integer, Vertex> vertexMap, Vertex vertex){
		ArrayList<Integer> nn = vertex.getOutNeighbors();
		nn.add(vertex.getVertexID());
		Collections.sort(nn);
		for(Integer neighbor : nn){
			//System.out.println("Unlocking "+neighbor);
			vertexMap.unlock(neighbor);
		}
		nn.remove(new Integer(vertex.getVertexID()));
	}

	public static void benchmark(IQueue<Integer> queue, IMap<Integer, Vertex> vertexMap, IAtomicLong counter){
		double elapsedTime=-1.0;
		long t1,t2;
		
		/////////////////  IMAP EXPERIMENTS  /////////////////////////
		getInput();
		t1 = System.nanoTime(); 
		for (Integer key : vertexMap.localKeySet()){
			Vertex vertex= vertexMap.get(key);
		}
		t2 = System.nanoTime(); 
		elapsedTime = (t2 - t1)/(Math.pow(10, 6));
		System.out.println("Getting all local vertices from map takes: "+elapsedTime/vertexMap.localKeySet().size()
				+"\t"+vertexMap.localKeySet().size());

		t1 = System.nanoTime(); 
		for (Integer key : vertexMap.keySet()){
			Vertex vertex= vertexMap.get(key);
		}
		t2 = System.nanoTime(); 
		elapsedTime = (t2 - t1)/(Math.pow(10, 6));
		System.out.println("Getting all vertices from map takes: "+elapsedTime/vertexMap.keySet().size()
				+"\t"+vertexMap.keySet().size());

		t1 = System.nanoTime(); 
		for (Integer key : vertexMap.localKeySet()){
			Vertex vertex= vertexMap.get(key);
			vertexMap.put(key, vertex);
		}
		t2 = System.nanoTime(); 
		elapsedTime = (t2 - t1)/(Math.pow(10, 6));
		System.out.println("Getting and putting all local vertices from map takes: "+elapsedTime/vertexMap.localKeySet().size()
				+"\t"+vertexMap.localKeySet().size());

		t1 = System.nanoTime(); 
		for (Integer key : vertexMap.keySet()){
			Vertex vertex= vertexMap.get(key);
			vertexMap.put(key, vertex);
		}
		t2 = System.nanoTime(); 
		elapsedTime = (t2 - t1)/(Math.pow(10, 6));
		System.out.println("Getting and putting all vertices from map takes: "+elapsedTime/vertexMap.keySet().size()
				+"\t"+vertexMap.keySet().size());
		
		t1 = System.nanoTime(); 
		for (Integer key : vertexMap.localKeySet()){
			vertexMap.lock(key);
		}
		t2 = System.nanoTime(); 
		elapsedTime = (t2 - t1)/(Math.pow(10, 6));
		System.out.println("Locking all local vertices from map takes: "+elapsedTime/vertexMap.localKeySet().size()
				+"\t"+vertexMap.localKeySet().size());

		t1 = System.nanoTime(); 
		for (Integer key : vertexMap.localKeySet()){
			vertexMap.unlock(key);
		}
		t2 = System.nanoTime(); 
		elapsedTime = (t2 - t1)/(Math.pow(10, 6));
		System.out.println("Unlocking all local vertices from map takes: "+elapsedTime/vertexMap.localKeySet().size()
				+"\t"+vertexMap.localKeySet().size());

		t1 = System.nanoTime(); 
		for (Integer key : vertexMap.keySet()){
			vertexMap.lock(key);
		}
		t2 = System.nanoTime(); 
		elapsedTime = (t2 - t1)/(Math.pow(10, 6));
		System.out.println("Locking all vertices from map takes: "+elapsedTime/vertexMap.keySet().size()
				+"\t"+vertexMap.keySet().size());
		
		t1 = System.nanoTime(); 
		for (Integer key : vertexMap.keySet()){
			vertexMap.unlock(key);
		}
		t2 = System.nanoTime(); 
		elapsedTime = (t2 - t1)/(Math.pow(10, 6));
		System.out.println("Unlocking all vertices from map takes: "+elapsedTime/vertexMap.keySet().size()
				+"\t"+vertexMap.keySet().size());
		
		
		/////////////////  QUEUE EXPERIMENTS  /////////////////////////
		getInput();

		System.out.println("Queue size is:"+queue.size()
				+" and "+queue.getLocalQueueStats().getOwnedItemCount());

		t1 = System.nanoTime(); 
		for (Integer key : vertexMap.keySet()){
			queue.add(key);
		}
		t2 = System.nanoTime(); 
		elapsedTime = (t2 - t1)/(Math.pow(10, 6));
		System.out.println("Adding all vertices to queue takes: "+elapsedTime/vertexMap.keySet().size()
				+"\t"+vertexMap.keySet().size());

		System.out.println("Queue size is:"+queue.size()
				+" and "+queue.getLocalQueueStats().getOwnedItemCount());
		
		t1 = System.nanoTime(); 
		for (Integer key : vertexMap.keySet()){
			queue.remove();
		}
		t2 = System.nanoTime(); 
		elapsedTime = (t2 - t1)/(Math.pow(10, 6));
		System.out.println("Removing all vertices from queue takes: "+elapsedTime/vertexMap.keySet().size()
				+"\t"+vertexMap.keySet().size());

		System.out.println("Queue size is:"+queue.size()
				+" and "+queue.getLocalQueueStats().getOwnedItemCount());
		
		t1 = System.nanoTime(); 
		for (Integer key : vertexMap.keySet()){
			if(!queue.contains(key)){
				//queue.add(key);
			}
		}
		t2 = System.nanoTime(); 
		elapsedTime = (t2 - t1)/(Math.pow(10, 6));
		System.out.println("Checking queue.contains takes: "+elapsedTime/vertexMap.keySet().size()
				+"\t"+vertexMap.keySet().size());
		
		t1 = System.nanoTime(); 
		for (Integer key : vertexMap.keySet()){
			int n = queue.size();
		}
		t2 = System.nanoTime(); 
		elapsedTime = (t2 - t1)/(Math.pow(10, 6));
		System.out.println("Checking queue.size takes: "+elapsedTime/vertexMap.keySet().size()
				+"\t"+vertexMap.keySet().size());
		
		
		/////////////// QUEUE.CONTAINS EXPERIMENT FOR DIFFERENT QUEUE SIZES ////////////////
//		getInput();
//		System.out.println("Queue size is:"+queue.size()
//				+" and "+queue.getLocalQueueStats().getOwnedItemCount());
//		queue.clear();
//		getInput();
//		Integer n = new Integer(-1);
//
//		for (int i=0; i<1000; i++){
//			queue.add(new Integer(i));
//		}
//		System.out.println("Queue size is:"+queue.size()
//				+" and "+queue.getLocalQueueStats().getOwnedItemCount());
//		
//		t1 = System.nanoTime(); 
//		for (int i=0; i<1000; i++){
//			if(!queue.contains(n)){	}
//		}
//		t2 = System.nanoTime(); 
//		elapsedTime = (t2 - t1)/(Math.pow(10, 6));
//		System.out.println("Checking queue.contains takes: "+elapsedTime/1000.0
//				+"\t"+vertexMap.keySet().size());
//
//
//		for (int i=1000; i<2000; i++){
//			queue.add(new Integer(i));
//		}
//		System.out.println("Queue size is:"+queue.size()
//				+" and "+queue.getLocalQueueStats().getOwnedItemCount());
//		
//		t1 = System.nanoTime(); 
//		for (int i=0; i<2000; i++){
//			if(!queue.contains(n)){	}
//		}
//		t2 = System.nanoTime(); 
//		elapsedTime = (t2 - t1)/(Math.pow(10, 6));
//		System.out.println("Checking queue.contains takes: "+elapsedTime/2000.0
//				+"\t"+vertexMap.keySet().size());
//		
//		
//		for (int i=2000; i<4000; i++){
//			queue.add(new Integer(i));
//		}
//		System.out.println("Queue size is:"+queue.size()
//				+" and "+queue.getLocalQueueStats().getOwnedItemCount());
//		
//		t1 = System.nanoTime(); 
//		for (int i=0; i<4000; i++){
//			if(!queue.contains(n)){	}
//		}
//		t2 = System.nanoTime(); 
//		elapsedTime = (t2 - t1)/(Math.pow(10, 6));
//		System.out.println("Checking queue.contains takes: "+elapsedTime/4000.0
//				+"\t"+vertexMap.keySet().size());
//
//
//		for (int i=4000; i<8000; i++){
//			queue.add(new Integer(i));
//		}
//		System.out.println("Queue size is:"+queue.size()
//				+" and "+queue.getLocalQueueStats().getOwnedItemCount());
//		
//		t1 = System.nanoTime(); 
//		for (int i=0; i<8000; i++){
//			if(!queue.contains(n)){	}
//		}
//		t2 = System.nanoTime(); 
//		elapsedTime = (t2 - t1)/(Math.pow(10, 6));
//		System.out.println("Checking queue.contains takes: "+elapsedTime/8000.0
//				+"\t"+vertexMap.keySet().size());
//
//
//		for (int i=8000; i<16000; i++){
//			queue.add(new Integer(i));
//		}
//		System.out.println("Queue size is:"+queue.size()
//				+" and "+queue.getLocalQueueStats().getOwnedItemCount());
//		
//		t1 = System.nanoTime(); 
//		for (int i=0; i<16000; i++){
//			if(!queue.contains(n)){	}
//		}
//		t2 = System.nanoTime(); 
//		elapsedTime = (t2 - t1)/(Math.pow(10, 6));
//		System.out.println("Checking queue.contains takes: "+elapsedTime/16000.0
//				+"\t"+vertexMap.keySet().size());
//
//		
//		for (int i=16000; i<32000; i++){
//			queue.add(new Integer(i));
//		}
//		System.out.println("Queue size is:"+queue.size()
//				+" and "+queue.getLocalQueueStats().getOwnedItemCount());
//		
//		t1 = System.nanoTime(); 
//		for (int i=0; i<32000; i++){
//			if(!queue.contains(n)){	}
//		}
//		t2 = System.nanoTime(); 
//		elapsedTime = (t2 - t1)/(Math.pow(10, 6));
//		System.out.println("Checking queue.contains takes: "+elapsedTime/32000.0
//				+"\t"+vertexMap.keySet().size());
//
//		
//		for (int i=32000; i<64000; i++){
//			queue.add(new Integer(i));
//		}
//		System.out.println("Queue size is:"+queue.size()
//				+" and "+queue.getLocalQueueStats().getOwnedItemCount());
//		
//		t1 = System.nanoTime(); 
//		for (int i=0; i<64000; i++){
//			if(!queue.contains(n)){	}
//		}
//		t2 = System.nanoTime(); 
//		elapsedTime = (t2 - t1)/(Math.pow(10, 6));
//		System.out.println("Checking queue.contains takes: "+elapsedTime/64000.0
//				+"\t"+vertexMap.keySet().size());
//		queue.clear();
//		getInput();

		
	/////////////// IATOMICLONG EXPERIMENTS ////////////////
		getInput();
		t1 = System.nanoTime(); 
		for (Integer key : vertexMap.keySet()){
			counter.get();
		}
		t2 = System.nanoTime(); 
		elapsedTime = (t2 - t1)/(Math.pow(10, 6));
		System.out.println("AtomicLong.get takes: "+elapsedTime/vertexMap.keySet().size()
				+"\t"+vertexMap.keySet().size());


		t1 = System.nanoTime(); 
		for (Integer key : vertexMap.keySet()){
			counter.set(24);
		}
		t2 = System.nanoTime(); 
		elapsedTime = (t2 - t1)/(Math.pow(10, 6));
		System.out.println("AtomicLong.set takes: "+elapsedTime/vertexMap.keySet().size()
				+"\t"+vertexMap.keySet().size());
		

		t1 = System.nanoTime(); 
		for (Integer key : vertexMap.keySet()){
			counter.incrementAndGet();
		}
		t2 = System.nanoTime(); 
		elapsedTime = (t2 - t1)/(Math.pow(10, 6));
		System.out.println("AtomicLong.incrementAndGet takes: "+elapsedTime/vertexMap.keySet().size()
				+"\t"+vertexMap.keySet().size());
		
		getInput();
	}

}
