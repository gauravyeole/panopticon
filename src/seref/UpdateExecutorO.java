package seref;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import seref.ServerEP.NeighborEntryProcessor;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;

public class UpdateExecutorO<T> implements Callable<Integer>, Serializable, HazelcastInstanceAware {
    
	private static final long serialVersionUID = 1L;
	private HazelcastInstance hz;
	private T keyM, key,key2;
	IMap<T, Vertex> vertexMap;
	IQueue<T> queue, nqueue;// = instance.getQueue("queue");
	IAtomicLong opCtr;// = instance.getAtomicLong("operationCounter");
	private double elapsedTime;
	private long t1, t2, myID;
	Class clazz;
	int p, nodeCount, partitionCount, wNum;
	EntryProcessor<Integer, Vertex> neighborProcessor;// = new NeighborEntryProcessor();
	EntryProcessor<PAKey, Vertex> neighborProcessorPA;// = new NeighborEntryProcessor();
	String job="ml";
	double val;


    public UpdateExecutorO() {
    }

    public UpdateExecutorO(T keyM, Class clazz, int nodeCount, int partitionCount, 
    		String job, long myID, int wNum, double val) {
        this.keyM = keyM;
        this.clazz = clazz;
        this.nodeCount = nodeCount;
        this.partitionCount = partitionCount;
		neighborProcessor = new NeighborEntryProcessor();
		neighborProcessorPA = new NeighborEntryProcessorPA();
		this.myID = myID;
		this.wNum = wNum;
		
        this.job = job;
        this.val = val;
    }

    
    public Integer call() {
    	vertexMap = hz.getMap("vertex");
        this.queue = hz.getQueue("queue@"+myID);
        this.opCtr = hz.getAtomicLong("operationCounter");

//        System.out.println("Map size in UE: "+vertexMap.size());
		Vertex vertex= vertexMap.get(keyM);

//    	lockNeighborhood(vertex);
		compute(vertexMap.get(keyM));	
//		unLockNeighborhood(vertex);
		
		return 0;
    }

	@Override
	public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
		this.hz = hazelcastInstance;
	}
	
	public int findP(int ID, int nodeCount, int partitionCount){
		double sqrtPC=(Math.floor(Math.sqrt(partitionCount)));
		double pn = Math.floor(Math.sqrt(nodeCount)/sqrtPC);
	    int column = (int) (ID%Math.floor(Math.sqrt(nodeCount)));
	    int row = (int) (Math.floor(ID/Math.sqrt(nodeCount)));
	    int p = (int) (Math.floor(row/pn)*sqrtPC + Math.floor(column/pn));
	    return p%wNum;
	}
	public void lockNeighborhood(Vertex vertex){
		ArrayList<Integer> nn = vertex.getOutNeighbors();
		nn.add(vertex.getVertexID());
		Collections.sort(nn);
		for(Integer neighbor : nn){
			//System.out.println("Locking "+neighbor);
			if(clazz == PAKey.class){
//				key = partitionMap.get(neighbor);
				p = findP(neighbor, nodeCount, partitionCount);
				key = (T) new PAKey(neighbor.intValue(), p+"");
			}else{
				key = (T) neighbor;
			}
			vertexMap.lock(key);
		}
		nn.remove(new Integer(vertex.getVertexID()));
	}
	public void unLockNeighborhood(Vertex vertex){
		ArrayList<Integer> nn = vertex.getOutNeighbors();
		nn.add(vertex.getVertexID());
		Collections.sort(nn);
		for(Integer neighbor : vertex.getOutNeighbors()){
			//System.out.println("Unlocking "+neighbor);
			if(clazz == PAKey.class){
//				key = partitionMap.get(neighbor);
				p = findP(neighbor, nodeCount, partitionCount);
				key = (T) new PAKey(neighbor.intValue(), p+"");
			}else{
				key = (T) neighbor;
			}
			vertexMap.unlock(key);
		}
		nn.remove(new Integer(vertex.getVertexID()));
	}

	public void compute(Vertex vertex){
		int nextColor = -1;
		int curColor = (int) vertex.getValue();

		int[] colorMap = new int[2*vertex.getOutNeighbors().size()];
		for(int i=0; i<colorMap.length; i++){ colorMap[i]=0;}
		System.out.println(vertex.getVertexID()+" -> "+ vertex.getValue()+":");

		for(Integer neighbor : vertex.getOutNeighbors()){
	    	
			if(clazz == PAKey.class){
//				key = partitionMap.get(neighbor);
				p = findP(neighbor, nodeCount, partitionCount);
				key = (T) new PAKey(neighbor.intValue(), p+"");
			}else{
				key = (T) neighbor;
			}
//			System.out.println("Reading neighbor: "+key+" "+neighbor+" "+p);
			
			int nColor=-1; 
			if(job.equals("ml")){
				nColor = (int) vertexMap.get(key).getValue();  // default
			}
			else if(job.equals("ep")){
				Double tmp=null;
				if(clazz == Integer.class){
					tmp = (Double) vertexMap.executeOnKey(key, neighborProcessor);  // for EP
				}
				else if(clazz == PAKey.class){
					tmp = (Double) vertexMap.executeOnKey(key, neighborProcessorPA);  // for EP
				}
				nColor = tmp.intValue();
			}
			else if(job.equals("de")){
				Callable<Double> task = new GetValueExecutorPA(key);
				IExecutorService executorService = hz.getExecutorService("default");
				Future<Double> result = executorService.submitToKeyOwner(task, key);
				try {
					nColor = result.get().intValue();
				} catch (InterruptedException | ExecutionException e) {e.printStackTrace();}
			}

			
			if(nColor < colorMap.length){
				colorMap[nColor]=1;
			}
		}
		for(int i=0; i<colorMap.length; i++){
			if(colorMap[i]!=1){
				nextColor=i;
//				System.out.println("Next color is:"+nextColor);
				break;
			}
		}
		if(curColor != nextColor){
			vertex.setValue(nextColor);
			if(clazz == PAKey.class){
				key2 = (T) new PAKey(vertex.getVertexID(), vertex.getPartitionID()+"");
//				System.out.println("PAKey:"+key2);
			}else{
				key2 = (T) new Integer(vertex.getVertexID());
			}
			vertexMap.put(key2, vertex);
//			System.out.println("Map size is:"+vertexMap.size());

			for(Integer neighbor : vertex.getOutNeighbors()){
				if(clazz == PAKey.class){
//					key = partitionMap.get(neighbor);
					p = findP(neighbor, nodeCount, partitionCount);
					key = (T) new PAKey(neighbor.intValue(), p+"");
				}else{
					key = (T) neighbor;
				}
				nqueue = hz.getQueue("queue@"+p);
				if(!nqueue.contains(key)){					
					nqueue.add(key);
				}
			}
		}

		long ctr = opCtr.incrementAndGet();
		//System.out.println("Committed "+vertex.getId());
		//System.out.println("Operation count is "+ctr);
	}
	
	static class NeighborEntryProcessor implements EntryProcessor<Integer, Vertex>, Serializable {
		private static final long serialVersionUID = 1L;
		public Object process(Map.Entry<Integer, Vertex> entry) {
			double value = entry.getValue().getValue();
			return new Double(value);
		}
		@Override
		public EntryBackupProcessor<Integer, Vertex> getBackupProcessor() {
			return null;
		}
	}
	
	static class NeighborEntryProcessorPA implements EntryProcessor<PAKey, Vertex>, Serializable {
		private static final long serialVersionUID = 1L;
		public Object process(Map.Entry<PAKey, Vertex> entry) {
			double value = entry.getValue().getValue();
			return new Double(value);
		}
		@Override
		public EntryBackupProcessor<PAKey, Vertex> getBackupProcessor() {
			return null;
		}
	}

	
}