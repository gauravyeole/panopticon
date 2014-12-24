package seref;
import com.hazelcast.config.Config;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.ManagementCenterConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

public class ServerEP {

	private static HazelcastInstance instance;
	private static int poolSize = 16;
	private static int numVertices = -1;
	private static boolean isQMap = true;
	static EntryProcessor<Integer, Vertex> entryProcessor;// = new IncrementorEntryProcessor();
	static EntryProcessor<Integer, Vertex> neighborProcessor;// = new NeighborEntryProcessor();
	static EntryProcessor<Integer, Vertex> neighborProcessorPR;// = new NeighborEntryProcessor();

	public static void initInstance(){
		Config cfg = new ClasspathXmlConfig("hazelcast.xml");
		ManagementCenterConfig managementCenterConfig = new ManagementCenterConfig();
		managementCenterConfig.setEnabled(true);
		managementCenterConfig.setUrl("http://localhost:8080/mancenter");
		cfg.setManagementCenterConfig(managementCenterConfig);
		cfg.getGroupConfig().setName("3.0");
		MapConfig mapCfg = new MapConfig();
		mapCfg.setName("vertex");
		mapCfg.setBackupCount(0);
		cfg.addMapConfig(mapCfg);
        cfg.getMapConfig("vertex").setInMemoryFormat(MapConfig.DEFAULT_IN_MEMORY_FORMAT.OBJECT);
		ExecutorConfig eCfg = new ExecutorConfig();
		eCfg.setPoolSize(poolSize);
		cfg.addExecutorConfig(eCfg);
		
		
		cfg.getSerializationConfig().addPortableFactory(1, new PortableVertexFactory());

		instance = Hazelcast.newHazelcastInstance(cfg);
	}

	
	public static void computePR(Vertex vertex, IMap<Integer, Vertex> vertexMap, 
			IQueue<Integer> queue, IAtomicLong opCtr){
		double sum = 0;
		double pr = -1;
		long t1, t2;
		double elapsedTime;
		double tmpr;

		for(Integer neighbor : vertex.getOutNeighbors()){
	    	//System.out.println("Reading "+neighbor);
			
//			CommonTools.getInput();
//			int nColor;
//	    	t1 = System.nanoTime(); 
//			Vertex vn = vertexMap.get(neighbor);
//			tmpr = vn.getValue()/vn.getOutNeighbors().size();
//			t2 = System.nanoTime(); 
//			elapsedTime = (t2 - t1)/(Math.pow(10, 6));
//			System.out.println("Single IMap.get takes: "+elapsedTime);
//
//			CommonTools.getInput();
//	    	t1 = System.nanoTime(); 
//			vertexMap.lock(neighbor);
//			vn = vertexMap.get(neighbor);
//			tmpr = vn.getValue()/vn.getOutNeighbors().size();
//			vertexMap.unlock(neighbor);
//			t2 = System.nanoTime(); 
//			elapsedTime = (t2 - t1)/(Math.pow(10, 6));
//			System.out.println("Single IMap.get with locks takes: "+elapsedTime);
//
//			CommonTools.getInput();
//	    	t1 = System.nanoTime(); 
//			tmpr = (Double) vertexMap.executeOnKey(neighbor, neighborProcessorPR);
//			t2 = System.nanoTime(); 
//			elapsedTime = (t2 - t1)/(Math.pow(10, 6));
//			System.out.println("Single EP.get takes: "+elapsedTime);

			
			tmpr = (Double) vertexMap.executeOnKey(neighbor, neighborProcessorPR);
//			Vertex vn = vertexMap.get(neighbor);
//			double tmpr = vn.getValue()/vn.getOutNeighbors().size();
			sum+=tmpr;
		}
		pr = (0.15f / numVertices) + 0.85f * sum;
		if(Math.abs(pr - vertex.getValue()) > 0.001){
			vertex.setValue(pr);
			vertexMap.lock(vertex.getVertexID());
			vertexMap.put(vertex.getVertexID(), vertex);
			vertexMap.unlock(vertex.getVertexID());
			for(Integer neighbor : vertex.getOutNeighbors()){
				if(!queue.contains(neighbor)){
					queue.add(neighbor);
				}
			}
		}
		
		long ctr = opCtr.incrementAndGet();
		//System.out.println("Committed "+vertex.getId());
		//System.out.println("Operation count is "+ctr);

	}

	public static void compute(Vertex vertex, IMap<Integer, Vertex> vertexMap, 
			IQueue<Integer> queue, IAtomicLong opCtr, IMap<Integer, Integer> qMap){
		int nextColor = -1;
		int curColor = (int) vertex.getValue();
		double elapsedTime;
		long t1, t2;

		int[] colorMap = new int[2*vertex.getOutNeighbors().size()];
		for(int i=0; i<colorMap.length; i++){ colorMap[i]=0;}
		//System.out.println(vertex.getId()+" -> "+ vertex.getValue()+":");

		ArrayList<Integer> nn = vertex.getOutNeighbors();
		Collections.sort(nn);
		for(Integer neighbor : nn){
	    	//System.out.println("Reading "+neighbor);
			int nColor;
			
//			CommonTools.getInput();
//	    	t1 = System.nanoTime(); 
//			nColor = (int) vertexMap.get(neighbor).getValue();
//			t2 = System.nanoTime(); 
//			elapsedTime = (t2 - t1)/(Math.pow(10, 6));
//			System.out.println("Single IMap.get takes: "+elapsedTime);
			
//	    	t1 = System.nanoTime(); 
			Double tmp = (Double) vertexMap.executeOnKey(neighbor, neighborProcessor);
//			t2 = System.nanoTime(); 
//			elapsedTime = (t2 - t1)/(Math.pow(10, 6));
//			System.out.println("Single EP for neighbor value takes: "+elapsedTime);

			nColor = tmp.intValue();
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
	

	public static void main(String[] args) {    
		double elapsedTime=-1.0;
		long t1,t2;
		String problem = "gc";
		if(args.length>0) problem = args[0];

		initInstance();
		CommonTools.waitLoading(instance);
		long startTime = System.nanoTime();    
		
		entryProcessor = new IncrementorEntryProcessor();
		neighborProcessor = new NeighborEntryProcessor();
		neighborProcessorPR = new NeighborEntryProcessorPR();
		IMap<Integer, Vertex> vertexMap = instance.getMap("vertex");
		IQueue<Integer> queue = instance.getQueue("queue");
		IAtomicLong counter = instance.getAtomicLong("counter");
		IAtomicLong opCtr = instance.getAtomicLong("operationCounter");
		IMap<Integer, Integer> qMap = instance.getMap("qMap");
		numVertices = vertexMap.size();

		for (Integer key : vertexMap.localKeySet()){
			//System.out.println("Vertex is: "+key+" -> "+ vertexMap.get(key).getValue());
			queue.add(key);
			if(isQMap) qMap.put(key, key);
		}
		
		while(queue.size()>0 || counter.get()>0 ){
			Integer key=null;
			key = (Integer) queue.poll();
			if(isQMap && key!=null) qMap.delete(key);

			if(key!=null){
				Vertex vertex= vertexMap.get(key);
				counter.incrementAndGet();	
				if(problem.equals("gc")){
					CommonTools.lockNeighborhood(vertexMap,vertex);
//					vertexMap.lock(key);
					compute(vertexMap.get(key), vertexMap, queue, opCtr, qMap);	
//					vertexMap.unlock(key);
//					vertexMap.executeOnKey(key, entryProcessor);
					CommonTools.unLockNeighborhood(vertexMap,vertex);
				}
				else{
					computePR(vertexMap.get(key), vertexMap, queue, opCtr);	
				}

				counter.decrementAndGet();
			}
			//System.out.println("Queue size is:"+queue.size()
			//		+" and "+queue.getLocalQueueStats().getOwnedItemCount());
			if(opCtr.get()%1000==0 && opCtr.get()>0){
				elapsedTime = (System.nanoTime() - startTime)/(Math.pow(10, 9));
				System.out.println(opCtr.get()+" Elapsed time is:"+elapsedTime);
			}
		}

		elapsedTime = (System.nanoTime() - startTime)/(Math.pow(10, 9));

		for (Integer key : vertexMap.localKeySet()){
			System.out.println("Vertex is: "+key+" -> "+ vertexMap.get(key).getValue());
		}
		System.out.println(opCtr.get()+" Total time is:"+elapsedTime);

		instance.getLifecycleService().shutdown();		
	}

	static class IncrementorEntryProcessor implements EntryProcessor<Integer, Vertex>, Serializable, HazelcastInstanceAware {
		private static final long serialVersionUID = 1L;
		transient HazelcastInstance hz;

		public Object process(Map.Entry<Integer, Vertex> entry) {
			Vertex vertex = entry.getValue();
			IMap<Integer, Vertex> vertexMap = instance.getMap("vertex");
			IQueue<Integer> queue = instance.getQueue("queue");
			IAtomicLong opCtr = instance.getAtomicLong("operationCounter");
			IMap<Integer, Integer> qMap = instance.getMap("qMap");
			
			ServerEP.compute(vertex, vertexMap, queue, opCtr, qMap);
			return null;
		}

		@Override
		public EntryBackupProcessor<Integer, Vertex> getBackupProcessor() {
			return null;
		}

		@Override
		public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
			this.hz = hazelcastInstance;
		}
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

	static class NeighborEntryProcessorPR implements EntryProcessor<Integer, Vertex>, Serializable {
		private static final long serialVersionUID = 1L;
		public Object process(Map.Entry<Integer, Vertex> entry) {
			double value = entry.getValue().getValue() / entry.getValue().getOutNeighbors().size();
			return new Double(value);
		}
		@Override
		public EntryBackupProcessor<Integer, Vertex> getBackupProcessor() {
			return null;
		}
	}

}
