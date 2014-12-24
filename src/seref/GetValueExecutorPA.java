package seref;
import java.io.Serializable;
import java.util.concurrent.Callable;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;

public class GetValueExecutorPA<T> implements Callable<Double>, Serializable, HazelcastInstanceAware {
    
	private static final long serialVersionUID = 1L;
	private HazelcastInstance hz;
	private T key;
	private double elapsedTime;
	private long t1, t2;

    public GetValueExecutorPA() {
    }

    public GetValueExecutorPA(T key) {
        this.key = key;
    }

    
    public Double call() {
    	IMap<Integer, Vertex> vertexMap = hz.getMap("vertex");

//    	t1 = System.nanoTime(); 
		double val = vertexMap.get(key).getValue();
//		t2 = System.nanoTime(); 
//		elapsedTime = (t2 - t1)/(Math.pow(10, 6));
//		System.out.println("Single IMap.get in getValueExecutor takes: "+elapsedTime);

    	return val;
    }

	@Override
	public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
		this.hz = hazelcastInstance;
	}
}