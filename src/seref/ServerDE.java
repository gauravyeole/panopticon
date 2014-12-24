package seref;
import com.hazelcast.config.Config;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.ManagementCenterConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ServerDE {

	private static HazelcastInstance instance;
	private static int poolSize = 16;
	private static boolean isQMap = true;

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
		System.setProperty("hazelcast.partition.count", "2");
		cfg.getSerializationConfig().addPortableFactory(1, new PortableVertexFactory());
		instance = Hazelcast.newHazelcastInstance(cfg);
	}

	public static void main(String[] args) {    
		String method = "doubleDE";
		if(args.length>0) method = args[0];
		if(args.length>1) poolSize = Integer.parseInt(args[1]);
		
		initInstance();
		CommonTools.waitLoading(instance);
		
		long startTime = System.nanoTime();  
		double elapsedTime;
		IExecutorService updateExecutorService = instance.getExecutorService("");

		IMap<Integer, Vertex> vertexMap = instance.getMap("vertex");
		IQueue<Integer> queue = instance.getQueue("queue@2");
		IAtomicLong counter = instance.getAtomicLong("counter@1");
		IAtomicLong opCtr = instance.getAtomicLong("operationCounter@3");
		IMap<Integer, Integer> qMap = instance.getMap("qMap");
		
		System.out.println("Partition of queue is: "+ queue.getPartitionKey());
		System.out.println("Local entry count is: "+ vertexMap.localKeySet().size());
		for (Integer key : vertexMap.localKeySet()){
			//System.out.println("Vertex is: "+key+" -> "+ vertexMap.get(key).getValue());
			queue.add(key);
			if(isQMap) qMap.put(key, key);
		}

		while(queue.size()>0 || counter.get()>0 ){
//			CommonTools.getInput();
			Integer key=null;
			key = (Integer) queue.poll();
			if(isQMap && key!=null) qMap.delete(key);

			if(key!=null){
				counter.incrementAndGet();	
				Future<Integer> future = updateExecutorService.submitToKeyOwner(new UpdateExecutor(key), key);
				try { future.get();
				} catch (InterruptedException | ExecutionException e) {e.printStackTrace();}	
				counter.decrementAndGet();
			}
			//System.out.println("Queue size is:"+queue.size()+" and "+queue.getLocalQueueStats().getOwnedItemCount());
			if(opCtr.get()%1000==0 && opCtr.get()>0){
				elapsedTime = (System.nanoTime() - startTime)/(Math.pow(10, 9));
				System.out.println(opCtr.get()+"Elapsed time is:"+elapsedTime);
			}
		}
		updateExecutorService.shutdown();
		try {
			updateExecutorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException e) {
			System.out.println("Error in waiting for termination of DE. Error is: "+e);
		}

		elapsedTime = (System.nanoTime() - startTime)/(Math.pow(10, 9));

		for (Integer key : vertexMap.localKeySet()){
			System.out.println("Vertex is: "+key+" -> "+ vertexMap.get(key).getValue());
		}	

		System.out.println(opCtr.get()+" Total time is:"+elapsedTime);
		instance.getLifecycleService().shutdown();

	}
}
