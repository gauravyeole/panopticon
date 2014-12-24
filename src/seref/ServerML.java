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
import com.hazelcast.config.ClasspathXmlConfig;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ServerML {

	private static HazelcastInstance instance;
	private static int poolSize = 16;
	private static int numVertices = -1;
	private static boolean isQMap = false;

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
		QueueConfig qCfg = new QueueConfig();
		qCfg.setName("queue");
		qCfg.setBackupCount(0);
		cfg.addQueueConfig(qCfg);
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
		for(Integer neighbor : vertex.getOutNeighbors()){
	    	//System.out.println("Reading "+neighbor);
			Vertex vn = vertexMap.get(neighbor);
			double tmpr = vn.getValue()/vn.getOutNeighbors().size();
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

		int[] colorMap = new int[2*vertex.getOutNeighbors().size()];
		for(int i=0; i<colorMap.length; i++){ colorMap[i]=0;}
		//System.out.println(vertex.getId()+" -> "+ vertex.getValue()+":");

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
		String method = "nonlocal";
		String problem = "gc";
		if(args.length>0) problem = args[0];
		if(args.length>1){
			if(args[1].equals("t"))
				isQMap = true;
		}
		double elapsedTime=-1.0;
		long t1,t2;
		initInstance();
		CommonTools.waitLoading(instance);
		
		IMap<Integer, Vertex> vertexMap = instance.getMap("vertex");
		IQueue<Integer> queue = instance.getQueue("queue");
		IMap<Integer, Integer> qMap = instance.getMap("qMap");

		IAtomicLong counter = instance.getAtomicLong("counter");
		IAtomicLong opCtr = instance.getAtomicLong("operationCounter");

		numVertices = vertexMap.size();
		Set<Integer> localKeySet = vertexMap.localKeySet();
		for (Integer key : localKeySet){
			//System.out.println("Vertex is: "+key+" -> "+ vertexMap.get(key).getValue());
			queue.add(key);
			if(isQMap) qMap.put(key, key);
		}
		
		
//		CommonTools.benchmark(queue, vertexMap, counter);
//		CommonTools.getInput();
		long startTime = System.nanoTime();    

        ExecutorService executor = Executors.newFixedThreadPool(16);

		int ctr=0;
		while(queue.size()>0 || counter.get()>0 ){
			Integer key=null;
			if(queue.size()>0){
				key = (Integer) queue.poll();
				if(isQMap && key!=null) qMap.delete(key);
				if(method.equals("local")){
//					t1 = System.nanoTime(); 
					boolean exist = localKeySet.contains(key);
//					t2 = System.nanoTime(); 
//					elapsedTime = (t2 - t1)/(Math.pow(10, 6));
//					System.out.println("Checking copied localKeySet takes: "+elapsedTime
//							+"\t"+vertexMap.keySet().size());
					if(!exist){
//						t1 = System.nanoTime(); 
						queue.add(key);
						key=null;
//						t2 = System.nanoTime(); 
//						elapsedTime = (t2 - t1)/(Math.pow(10, 6));
//						System.out.println("Adding to queue takes: "+elapsedTime
//								+"\t"+queue.size());
					}
				}
			}
//			if(ctr++ % 10 == 0){
//				CommonTools.getInput();
//			}

			if(key!=null){
				Vertex vertex= vertexMap.get(key);
				counter.incrementAndGet();	
//				Runnable worker = new WorkerThread(vertexMap.get(key), vertexMap, queue, opCtr, key);
//	            executor.execute(worker);
				if(problem.equals("gc")){
					CommonTools.lockNeighborhood(vertexMap,vertex);
//					vertexMap.lock(key);
					compute(vertexMap.get(key), vertexMap, queue, opCtr, qMap);	
//					vertexMap.unlock(key);
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
//		executor.shutdown();
//        while (!executor.isTerminated()) {}

		elapsedTime = (System.nanoTime() - startTime)/(Math.pow(10, 9));

		for (Integer key : vertexMap.localKeySet()){
			System.out.println("Vertex is: "+key+" -> "+ vertexMap.get(key).getValue());
		}
		
		System.out.println(opCtr.get()+" Total time is:"+elapsedTime);
		instance.getLifecycleService().shutdown();
	}
	
	
	
	public static class WorkerThread implements Runnable {
		Vertex vertex; 
		IMap<Integer, Vertex> vertexMap;
		IQueue<Integer> queue; 
		IAtomicLong opCtr;
		Integer key;

		public WorkerThread(Vertex vertex, IMap<Integer, Vertex> vertexMap,
				IQueue<Integer> queue, IAtomicLong opCtr, Integer key) {
			this.vertex= vertex;
			this.vertexMap = vertexMap;
			this.opCtr = opCtr;
			this.queue = queue;
			this.key = key;	
		}

		@Override
	    public void run() {
	    	CommonTools.lockNeighborhood(vertexMap,vertex);
			compute(vertexMap.get(key), vertexMap, queue, opCtr, null);	
			CommonTools.unLockNeighborhood(vertexMap,vertex);
	    }

	}

}
