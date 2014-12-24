package seref;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;

public class UpdateExecutor implements Callable<Integer>, Serializable, HazelcastInstanceAware {
    
	private static final long serialVersionUID = 1L;
	private HazelcastInstance hz;
	private Integer key;
	IQueue<Integer> queue;// = instance.getQueue("queue");
	IAtomicLong opCtr;// = instance.getAtomicLong("operationCounter");
	IMap<Integer, Integer> qMap;
	private double elapsedTime;
	private long t1, t2;
	private static boolean isQMap = true;

    public UpdateExecutor() {
    }

    public UpdateExecutor(Integer key) {
        this.key = key;
    }

    
    public Integer call() {
    	IMap<Integer, Vertex> vertexMap = hz.getMap("vertex");
        this.queue = hz.getQueue("queue");
        this.opCtr = hz.getAtomicLong("operationCounter");
        this.qMap = hz.getMap("qMap");

		Vertex vertex= vertexMap.get(key);

    	CommonTools.lockNeighborhood(vertexMap,vertex);
//    	vertexMap.lock(key);
		compute(vertexMap.get(key), vertexMap, queue, opCtr);	
//		vertexMap.unlock(key);
		CommonTools.unLockNeighborhood(vertexMap,vertex);
		
		return 0;
    }

	@Override
	public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
		this.hz = hazelcastInstance;
	}
	
	public void compute(Vertex vertex, IMap<Integer, Vertex> vertexMap, 
			IQueue<Integer> queue, IAtomicLong opCtr){
		int nextColor = -1;
		int curColor = (int) vertex.getValue();
		ArrayList<Future<Double>> futures = new ArrayList<Future<Double>>();

		int[] colorMap = new int[2*vertex.getOutNeighbors().size()];
		for(int i=0; i<colorMap.length; i++){ colorMap[i]=0;}
		//System.out.println(vertex.getId()+" -> "+ vertex.getValue()+":");

		// Method 1: Use DE
		for(Integer neighbor : vertex.getOutNeighbors()){
			try {
				futures.add(echoOnTheMemberOwningTheKey(neighbor, neighbor));
			} catch (Exception e) {e.printStackTrace();}
		}
		for (Future<Double> future : futures) {
			int nColor = 0;
			try {
				nColor = future.get().intValue();
			} catch (InterruptedException | ExecutionException e) {e.printStackTrace();}
			//System.out.println("Color is "+nColor);
			if(nColor < colorMap.length){
				colorMap[nColor]=1;
			}
		}
		
		// Method 2: Use Imap.get
//		for(Integer neighbor : vertex.getOutNeighbors()){
//	    	//System.out.println("Reading "+neighbor);
//			int nColor = (int) vertexMap.get(neighbor).getValue();
//			if(nColor < colorMap.length){
//				colorMap[nColor]=1;
//			}
//		}
		
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
			
			for(Integer neighbor : vertex.getOutNeighbors()){
				if(isQMap){
					if(qMap.get(neighbor)==null){
						queue.offer(neighbor);
						qMap.put(neighbor, neighbor);
					}
				}else{
					if(!queue.contains(neighbor)){
						queue.add(neighbor);
					}
				}
			}
		}

		long ctr = opCtr.incrementAndGet();
		//System.out.println("Committed "+vertex.getId());
		//System.out.println("Operation count is "+ctr);
	}

	public Future<Double> echoOnTheMemberOwningTheKey(Integer nkey, Object key) throws Exception {
		Callable<Double> task = new GetValueExecutor(nkey);
		IExecutorService executorService = hz.getExecutorService("default");
//		t1 = System.nanoTime(); 
		Future<Double> result = executorService.submitToKeyOwner(task, key);
		result.get();
//		t2 = System.nanoTime(); 
//		elapsedTime = (t2 - t1)/(Math.pow(10, 6));
//		System.out.println("Single getValueExecutor call takes: "+elapsedTime);

		return result;
	}

}